/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorSplitSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;

import javax.inject.Inject;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static io.trino.plugin.iceberg.IcebergSessionProperties.getDynamicFilteringWaitTimeout;
import static io.trino.plugin.iceberg.IcebergSessionProperties.getMinimumAssignedSplitWeight;
import static io.trino.spi.connector.FixedSplitSource.emptySplitSource;
import static java.util.Objects.requireNonNull;

public class IcebergSplitManager
        implements ConnectorSplitManager
{
    public static final int ICEBERG_DOMAIN_COMPACTION_THRESHOLD = 1000;

    private final IcebergTransactionManager transactionManager;
    private final TypeManager typeManager;
    private final TrinoFileSystemFactory fileSystemFactory;

    @Inject
    public IcebergSplitManager(IcebergTransactionManager transactionManager, TypeManager typeManager, TrinoFileSystemFactory fileSystemFactory)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle handle,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        IcebergTableHandle table = (IcebergTableHandle) handle;

        if (table.getSnapshotId().isEmpty()) {
            if (table.isRecordScannedFiles()) {
                return new FixedSplitSource(ImmutableList.of(), ImmutableList.of());
            }
            return emptySplitSource();
        }

        Table icebergTable = transactionManager.get(transaction, session.getIdentity()).getIcebergTable(session, table.getSchemaTableName());
        Duration dynamicFilteringWaitTimeout = getDynamicFilteringWaitTimeout(session);

        TableScan tableScan = icebergTable.newScan()
                .useSnapshot(table.getSnapshotId().get());
        IcebergSplitSource splitSource = new IcebergSplitSource(
                fileSystemFactory,
                session,
                table,
                tableScan,
                table.getMaxScannedFileSize(),
                dynamicFilter,
                dynamicFilteringWaitTimeout,
                constraint,
                typeManager,
                table.isRecordScannedFiles(),
                getMinimumAssignedSplitWeight(session));

        return new ClassLoaderSafeConnectorSplitSource(new SortingSplitSource(splitSource), IcebergSplitManager.class.getClassLoader());
    }

    static class SortingSplitSource
            implements ConnectorSplitSource
    {
        private final ConnectorSplitSource delegate;
        private List<ConnectorSplit> splits;
        private int offset;

        SortingSplitSource(ConnectorSplitSource delegate)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
        }

        @Override
        public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize)
        {
            if (splits == null) {
                return delegate.getNextBatch(Integer.MAX_VALUE).thenApply(allSplits -> {
                    splits = allSplits.getSplits().stream().sorted(Comparator.comparing(split -> split.getInfo().toString())).collect(Collectors.toList());
                    int endIndex = Math.min(maxSize, splits.size());
                    offset = maxSize;
                    return new ConnectorSplitBatch(splits.subList(0, endIndex), endIndex == splits.size());
                });
            }

            int startIndex = offset;
            int endIndex = Math.min(startIndex + maxSize, splits.size());
            offset = endIndex;

            return CompletableFuture.completedFuture(new ConnectorSplitBatch(splits.subList(startIndex, endIndex), endIndex == splits.size()));
        }

        @Override
        public void close()
        {
            delegate.close();
        }

        @Override
        public boolean isFinished()
        {
            return delegate.isFinished();
        }

        @Override
        public Optional<List<Object>> getTableExecuteSplitsInfo()
        {
            return delegate.getTableExecuteSplitsInfo();
        }
    }
}

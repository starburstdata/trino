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
package io.trino.plugin.hive.cache;

import com.google.common.collect.ImmutableList;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import io.airlift.units.DataSize;
import io.trino.plugin.hive.HiveSplit;
import io.trino.plugin.hive.HiveSplitWeightProvider;
import io.trino.plugin.hive.util.SizeBasedSplitWeightProvider;
import io.trino.spi.Node;
import io.trino.spi.connector.ConnectorPartitionHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitSource;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.hive.HiveSessionProperties.getMaxSplitSize;
import static io.trino.plugin.hive.HiveSessionProperties.getMinimumAssignedSplitWeight;
import static io.trino.plugin.hive.HiveSessionProperties.isMergeSplits;
import static io.trino.plugin.hive.HiveSessionProperties.isSizeBasedSplitWeightsEnabled;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.groupingBy;

public class CacheSplitSource
        implements ConnectorSplitSource
{
    private final Supplier<List<Node>> orderedWorkerNodes;
    private final ConnectorSplitSource delegate;
    private final boolean mergeSplits;
    private final DataSize maxSplitSize;
    private final HiveSplitWeightProvider splitWeightProvider;

    public CacheSplitSource(ConnectorSession session, Supplier<List<Node>> orderedWorkerNodes, ConnectorSplitSource delegate)
    {
        this.orderedWorkerNodes = requireNonNull(orderedWorkerNodes, "orderedWorkerNodes is null");
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.mergeSplits = isMergeSplits(session);
        this.maxSplitSize = getMaxSplitSize(session);
        this.splitWeightProvider = isSizeBasedSplitWeightsEnabled(session) ? new SizeBasedSplitWeightProvider(getMinimumAssignedSplitWeight(session), maxSplitSize) : HiveSplitWeightProvider.uniformStandardWeightProvider();
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(ConnectorPartitionHandle partitionHandle, int maxSize)
    {
        return delegate.getNextBatch(partitionHandle, maxSize).thenApply(batch -> new ConnectorSplitBatch(
                batch.getSplits().stream()
                        .flatMap(split -> unnestSplit((HiveSplit) split).stream())
                        .map(this::getCacheSplit)
                        .collect(groupingBy(
                                split -> getOnlyElement(split.getAddresses()),
                                collectingAndThen(toImmutableList(), this::mergeSplits)))
                        .values().stream()
                        .flatMap(Collection::stream)
                        .collect(toImmutableList()),
                batch.isNoMoreSplits()));
    }

    private List<HiveSplit> mergeSplits(List<HiveSplit> splits)
    {
        ImmutableList.Builder<HiveSplit> resultBuilder = ImmutableList.builder();
        Optional<HiveSplit> mergedSplit = Optional.empty();
        for (HiveSplit split : splits) {
            mergedSplit = Optional.of(mergeSplits(mergedSplit, split));
            if (mergedSplit.get().getChainLength() >= maxSplitSize.toBytes() || !mergeSplits) {
                resultBuilder.add(mergedSplit.get());
                mergedSplit = Optional.empty();
            }
        }

        mergedSplit.ifPresent(resultBuilder::add);
        return resultBuilder.build();
    }

    private HiveSplit mergeSplits(Optional<HiveSplit> mergedSplit, HiveSplit split)
    {
        return mergedSplit.map(hiveSplit -> new HiveSplit(
                        split.getDatabase(),
                        split.getTable(),
                        split.getPartitionName(),
                        split.getPath(),
                        split.getStart(),
                        split.getLength(),
                        split.getEstimatedFileSize(),
                        split.getFileModifiedTime(),
                        split.getSchema(),
                        split.getPartitionKeys(),
                        split.getAddresses(),
                        split.getBucketNumber(),
                        split.getStatementId(),
                        split.isForceLocalScheduling(),
                        split.getTableToPartitionMapping(),
                        split.getBucketConversion(),
                        split.getBucketValidation(),
                        split.isS3SelectPushdownEnabled(),
                        split.getAcidInfo(),
                        split.getSplitNumber(),
                        splitWeightProvider.weightForSplitSizeInBytes(split.getLength() + hiveSplit.getChainLength()),
                        Optional.of(hiveSplit)))
                .orElseGet(() -> new HiveSplit(
                        split.getDatabase(),
                        split.getTable(),
                        split.getPartitionName(),
                        split.getPath(),
                        split.getStart(),
                        split.getLength(),
                        split.getEstimatedFileSize(),
                        split.getFileModifiedTime(),
                        split.getSchema(),
                        split.getPartitionKeys(),
                        split.getAddresses(),
                        split.getBucketNumber(),
                        split.getStatementId(),
                        split.isForceLocalScheduling(),
                        split.getTableToPartitionMapping(),
                        split.getBucketConversion(),
                        split.getBucketValidation(),
                        split.isS3SelectPushdownEnabled(),
                        split.getAcidInfo(),
                        split.getSplitNumber(),
                        splitWeightProvider.weightForSplitSizeInBytes(split.getLength()),
                        Optional.empty()));
    }

    private List<HiveSplit> unnestSplit(HiveSplit split)
    {
        ImmutableList.Builder<HiveSplit> splits = ImmutableList.builder();
        splits.add(split);
        Optional<HiveSplit> currentSplit = split.getNextSplit();
        while (currentSplit.isPresent()) {
            splits.add(currentSplit.get());
            currentSplit = currentSplit.get().getNextSplit();
        }
        return splits.build();
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

    private HiveSplit getCacheSplit(HiveSplit split)
    {
        SplitSignature signature = new SplitSignature(split.getPath(), split.getStart(), split.getLength(), split.getFileModifiedTime());
        List<Node> workerNodes = orderedWorkerNodes.get();
        int bucket = Hashing.consistentHash(HashCode.fromInt(signature.hashCode()), workerNodes.size());
        //System.err.println("CACHING SPLIT TO WORKER: " + workerNodes.get(bucket) + " SPLIT: " + split);
        return new HiveSplit(
                split.getDatabase(),
                split.getTable(),
                split.getPartitionName(),
                split.getPath(),
                split.getStart(),
                split.getLength(),
                split.getEstimatedFileSize(),
                split.getFileModifiedTime(),
                split.getSchema(),
                split.getPartitionKeys(),
                ImmutableList.of(workerNodes.get(bucket).getHostAndPort()),
                split.getBucketNumber(),
                split.getStatementId(),
                true,
                split.getTableToPartitionMapping(),
                split.getBucketConversion(),
                split.getBucketValidation(),
                split.isS3SelectPushdownEnabled(),
                split.getAcidInfo(),
                split.getSplitNumber(),
                split.getSplitWeight(),
                Optional.empty());
    }
}

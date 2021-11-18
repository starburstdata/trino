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
package io.trino.plugin.hive;

import io.trino.spi.Page;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.DynamicFilter;

import java.io.IOException;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class ChainedHivePageSource
        implements ConnectorPageSource
{
    private final HivePageSourceProvider pageSourceProvider;
    private final ConnectorSession session;
    private final ConnectorTableHandle tableHandle;
    private final List<ColumnHandle> columns;
    private final DynamicFilter dynamicFilter;

    private HiveSplit split;
    private ConnectorPageSource pageSource;
    private long completedBytes;
    private long readTimeNanos;

    public ChainedHivePageSource(
            HivePageSourceProvider pageSourceProvider,
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter,
            ConnectorSplit split)
    {
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        this.session = requireNonNull(session, "session is null");
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.columns = requireNonNull(columns, "columns is null");
        this.dynamicFilter = requireNonNull(dynamicFilter, "dynamicFilter is null");
        this.split = (HiveSplit) requireNonNull(split, "split is null");
        this.pageSource = pageSourceProvider.createSingleSplitPageSource(session, split, tableHandle, columns, dynamicFilter);
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes + pageSource.getCompletedBytes();
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos + pageSource.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return pageSource.isFinished() && split.getNextSplit().isEmpty();
    }

    @Override
    public Page getNextPage()
    {
        if (pageSource.isFinished()) {
            completedBytes += pageSource.getCompletedBytes();
            readTimeNanos += pageSource.getReadTimeNanos();
            try {
                pageSource.close();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }

            if (split.getNextSplit().isEmpty()) {
                return null;
            }

            split = split.getNextSplit().get();
            pageSource = pageSourceProvider.createSingleSplitPageSource(session, split, tableHandle, columns, dynamicFilter);
        }

        return pageSource.getNextPage();
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return pageSource.getSystemMemoryUsage();
    }

    @Override
    public void close()
            throws IOException
    {
        pageSource.close();
    }
}

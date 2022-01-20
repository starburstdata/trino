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
package io.trino.execution;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedPageSource;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.MoreFutures.toCompletableFuture;
import static java.util.Objects.requireNonNull;

public class TestingPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final ListenableFuture<Void> producePage;

    public TestingPageSourceProvider(ListenableFuture<Void> producePage)
    {
        this.producePage = requireNonNull(producePage, "producePage is null");
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        requireNonNull(columns, "columns is null");

        ImmutableList<Block> blocks = columns.stream()
                .map(column -> new ByteArrayBlock(1, Optional.of(new boolean[] {true}), new byte[1]))
                .collect(toImmutableList());

        return new BlockingFixedPageSource(ImmutableList.of(new Page(blocks.toArray(new Block[blocks.size()]))));
    }

    private class BlockingFixedPageSource
            extends FixedPageSource
    {
        public BlockingFixedPageSource(Iterable<Page> pages)
        {
            super(pages);
        }

        @Override
        public CompletableFuture<?> isBlocked()
        {
            return toCompletableFuture(producePage);
        }

        @Override
        public boolean isFinished()
        {
            return producePage.isDone() && super.isFinished();
        }

        @Override
        public Page getNextPage()
        {
            if (!producePage.isDone()) {
                return null;
            }

            return super.getNextPage();
        }
    }
}

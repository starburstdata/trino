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
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.cache.WorkerCacheManager.CachedData;
import io.trino.plugin.hive.cache.serde.PagesSerde;
import io.trino.plugin.hive.cache.serde.PagesSerdeFactory;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.cache.CacheSessionProperties.isCacheCompressionEnabled;
import static io.trino.plugin.hive.cache.CacheSessionProperties.isCacheMergePages;
import static io.trino.plugin.hive.cache.CacheSessionProperties.isCacheUseRawFormat;
import static io.trino.plugin.hive.cache.serde.PagesSerdeUtil.writePages;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class CachePageSink
        implements ConnectorPageSink
{
    private final ConnectorSession session;
    private final SplitSignature signature;
    private final List<ColumnHandle> readerColumns;
    private final WorkerCacheManager cacheManager;
    private final PagesSerde pagesSerde;
    private final DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1_000);
    private final ImmutableList.Builder<Page> pages = new ImmutableList.Builder<>();
    private final List<Type> types;
    private final PageBuilder pageBuilder;
    private final boolean compressed;
    private final boolean mergePages;
    private final boolean useRawFormat;

    public CachePageSink(ConnectorSession session, SplitSignature signature, List<ColumnHandle> readerColumns, WorkerCacheManager cacheManager, PagesSerdeFactory pagesSerdeFactory)
    {
        this.session = requireNonNull(session, "session is null");
        this.signature = requireNonNull(signature, "signature is null");
        this.readerColumns = requireNonNull(readerColumns, "readerColumns is null");
        this.cacheManager = requireNonNull(cacheManager, "cacheManager is null");
        this.pagesSerde = pagesSerdeFactory.createPagesSerde(isCacheCompressionEnabled(session));
        this.types = readerColumns.stream()
                .map(column -> ((HiveColumnHandle) column).getType())
                .collect(toImmutableList());
        this.pageBuilder = PageBuilder.withMaxPageSize(4 * 1024 * 1024, types);
        this.mergePages = isCacheMergePages(session);
        this.useRawFormat = isCacheUseRawFormat(session);
        this.compressed = isCacheCompressionEnabled(session);
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        if (mergePages) {
            pageBuilder.declarePositions(page.getPositionCount());
            for (int channel = 0; channel < types.size(); channel++) {
                appendBlock(
                        types.get(channel),
                        page.getBlock(channel).getLoadedBlock(),
                        pageBuilder.getBlockBuilder(channel));
            }

            if (pageBuilder.isFull()) {
                flushPageBuilder();
            }
        }
        else {
            storePage(page);
        }

        return completedFuture(null);
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        if (!pageBuilder.isEmpty()) {
            flushPageBuilder();
        }
        if (useRawFormat) {
            cacheManager.addCachedData(session, signature, readerColumns, new CachedData(Optional.empty(), compressed, Optional.of(pages.build())));
        }
        else {
            cacheManager.addCachedData(session, signature, readerColumns, new CachedData(Optional.of(sliceOutput.slice()), compressed, Optional.empty()));
        }
        return completedFuture(ImmutableList.of());
    }

    @Override
    public void abort()
    {
    }

    private void flushPageBuilder()
    {
        storePage(pageBuilder.build());
        pageBuilder.reset();
    }

    private void storePage(Page page)
    {
        if (useRawFormat) {
            pages.add(page);
        }
        else {
            writePages(pagesSerde, sliceOutput, page);
        }
    }

    private void appendBlock(Type type, Block block, BlockBuilder blockBuilder)
    {
        for (int position = 0; position < block.getPositionCount(); position++) {
            type.appendTo(block, position, blockBuilder);
        }
    }
}

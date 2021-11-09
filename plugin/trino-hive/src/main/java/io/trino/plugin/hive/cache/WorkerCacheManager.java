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

import io.airlift.slice.Slice;
import io.trino.plugin.hive.cache.serde.CachePagesSerdeFactory;
import io.trino.spi.HostAddress;
import io.trino.spi.NodeManager;
import io.trino.spi.Page;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.TypeManager;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.hive.cache.CacheSessionProperties.getCacheMaxVersions;
import static io.trino.plugin.hive.cache.CacheSessionProperties.isCacheEnabled;
import static java.util.Objects.requireNonNull;

public class WorkerCacheManager
{
    private final HostAddress currentNode;
    private final Map<SplitSignature, Map<List<ColumnHandle>, CachedData>> cachedData = new ConcurrentHashMap<>();
    private final CachePagesSerdeFactory pagesSerdeFactory;

    @Inject
    public WorkerCacheManager(CacheConfig cacheConfig, NodeManager nodeManager, TypeManager typeManager)
    {
        this.currentNode = requireNonNull(nodeManager, "nodeManager is null").getCurrentNode().getHostAndPort();
        this.pagesSerdeFactory = new CachePagesSerdeFactory(requireNonNull(typeManager, "typeManager is null"));
    }

    public boolean shouldCacheData(ConnectorSession session, List<HostAddress> splitAddresses, SplitSignature signature, List<ColumnHandle> readerColumns)
    {
        if (!isCacheEnabled(session)) {
            System.err.println("DON'T CACHE (cache disabled): " + splitAddresses + " CURRENT NODE: " + currentNode + " SIG: " + signature + " COLUMNS: " + readerColumns);
            return false;
        }

        if (splitAddresses.size() != 1 || !getOnlyElement(splitAddresses).equals(currentNode)) {
            System.err.println("DON'T CACHE (address don't match): " + splitAddresses + " CURRENT NODE: " + currentNode + " SIG: " + signature + " COLUMNS: " + readerColumns);
            return false;
        }

        if (cachedData.containsKey(signature) && cachedData.get(signature).containsKey(readerColumns)) {
            System.err.println("DON'T CACHE (already present): " + splitAddresses + " CURRENT NODE: " + currentNode + " SIG: " + signature + " COLUMNS: " + readerColumns);
            return false;
        }

        System.err.println("SHOULD CACHE: " + splitAddresses + " CURRENT NODE: " + currentNode + " SIG: " + signature + " COLUMNS: " + readerColumns);
        return true;
    }

    public ConnectorPageSink getCachePageSink(ConnectorSession session, SplitSignature signature, List<ColumnHandle> readerColumns)
    {
        System.err.println("GETTING PAGE SINK " + signature + " COLUMNS: " + readerColumns);
        return new CachePageSink(session, signature, readerColumns, this, pagesSerdeFactory);
    }

    public Optional<ConnectorPageSource> getCachePageSource(ConnectorSession session, SplitSignature signature, List<ColumnHandle> readerColumns)
    {
        if (!isCacheEnabled(session)) {
            return Optional.empty();
        }

        Optional<ConnectorPageSource> pageSource = Optional.ofNullable(cachedData.get(signature))
                .flatMap(map -> Optional.ofNullable(map.get(readerColumns)))
                .map(data -> new CachePageSource(pagesSerdeFactory, data));

        if (pageSource.isPresent()) {
            //System.err.println("PRESENT SOURCE FOR " + signature + " COLUMNS: " + readerColumns);
        }
        else {
            System.err.println("NO PAGE SOURCE FOR " + signature + " COLUMNS: " + readerColumns);
        }

        return pageSource;
    }

    void addCachedData(ConnectorSession session, SplitSignature signature, List<ColumnHandle> readerColumns, CachedData data)
    {
        System.err.println("ADD CACHE DATA " + signature + " COLUMNS: " + readerColumns);
        Map<List<ColumnHandle>, CachedData> versions = cachedData.computeIfAbsent(signature, ignored -> new ConcurrentHashMap<>());
        if (versions.size() >= getCacheMaxVersions(session)) {
            versions.clear();
        }
        versions.put(readerColumns, data);
    }

    public static class CachedData
    {
        private final Optional<Slice> slice;
        private final boolean compressed;
        private final Optional<List<Page>> pages;

        public CachedData(Optional<Slice> slice, boolean compressed, Optional<List<Page>> pages)
        {
            this.slice = requireNonNull(slice, "slice is null");
            this.compressed = compressed;
            this.pages = requireNonNull(pages, "pages is null");
        }

        public Optional<Slice> getSlice()
        {
            return slice;
        }

        public boolean isCompressed()
        {
            return compressed;
        }

        public Optional<List<Page>> getPages()
        {
            return pages;
        }
    }
}

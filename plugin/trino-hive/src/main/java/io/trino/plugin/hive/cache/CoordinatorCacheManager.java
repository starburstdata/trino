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

import com.google.common.base.Suppliers;
import io.trino.spi.Node;
import io.trino.spi.NodeManager;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitSource;

import javax.inject.Inject;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.cache.CacheSessionProperties.isCacheEnabled;

public class CoordinatorCacheManager
{
    private final Supplier<List<Node>> orderedWorkerNodes;

    @Inject
    public CoordinatorCacheManager(NodeManager nodeManager)
    {
        this.orderedWorkerNodes = Suppliers.memoizeWithExpiration(
                () -> nodeManager.getWorkerNodes().stream()
                        .sorted(Comparator.comparing(node -> node.getHostAndPort().toString()))
                        .collect(toImmutableList()),
                10,
                TimeUnit.SECONDS);
    }

    public ConnectorSplitSource getCacheSplitSource(ConnectorSession session, ConnectorSplitSource delegate)
    {
        if (!isCacheEnabled(session)) {
            return delegate;
        }

        return new CacheSplitSource(session, orderedWorkerNodes, delegate);
    }
}

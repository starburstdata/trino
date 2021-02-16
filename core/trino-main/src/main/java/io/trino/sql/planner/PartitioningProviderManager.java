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
package io.trino.sql.planner;

import io.trino.connector.CatalogName;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class PartitioningProviderManager
{
    private final ConcurrentMap<CatalogName, ConnectorNodePartitioningProvider> partitioningProviders = new ConcurrentHashMap<>();

    public ConnectorNodePartitioningProvider getPartitioningProvider(CatalogName catalogName)
    {
        ConnectorNodePartitioningProvider partitioningProvider = partitioningProviders.get(catalogName);
        checkArgument(partitioningProvider != null, "No partitioning provider for connector %s", catalogName);
        return partitioningProvider;
    }

    public void addPartitioningProvider(CatalogName catalogName, ConnectorNodePartitioningProvider nodePartitioningProvider)
    {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(nodePartitioningProvider, "nodePartitioningProvider is null");
        checkArgument(partitioningProviders.putIfAbsent(catalogName, nodePartitioningProvider) == null,
                "NodePartitioningProvider for connector '%s' is already registered", catalogName);
    }

    public void removePartitioningProvider(CatalogName catalogName)
    {
        partitioningProviders.remove(catalogName);
    }
}

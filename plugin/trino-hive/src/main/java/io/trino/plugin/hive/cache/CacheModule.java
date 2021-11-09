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

import com.google.inject.Binder;
import com.google.inject.Module;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.connector.ConnectorContext;

import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class CacheModule
        implements Module
{
    private final boolean isCoordinator;

    public CacheModule(ConnectorContext context)
    {
        isCoordinator = context.getNodeManager().getCurrentNode().isCoordinator();
    }

    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(CacheConfig.class);
        newSetBinder(binder, SessionPropertiesProvider.class).addBinding()
                .to(CacheSessionProperties.class).in(SINGLETON);
        binder.bind(CoordinatorCacheManager.class).in(SINGLETON);
        binder.bind(WorkerCacheManager.class).in(SINGLETON);
    }
}

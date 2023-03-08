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
package io.trino.plugin.deltalake.metastore.spark;

import com.amazonaws.services.glue.model.Table;
import io.trino.plugin.deltalake.metastore.glue.DeltaLakeGlueMetastoreConfig;
import io.trino.plugin.hive.metastore.glue.DefaultGlueMetastoreTableFilterProvider;

import javax.inject.Inject;
import javax.inject.Provider;

import java.util.function.Predicate;

public class DeltaLakeSparkMetastoreTableFilterProvider
        implements Provider<Predicate<Table>>
{
    private final boolean hideNonDeltaLakeTables;

    @Inject
    public DeltaLakeSparkMetastoreTableFilterProvider(DeltaLakeGlueMetastoreConfig config)
    {
        this.hideNonDeltaLakeTables = config.isHideNonDeltaLakeTables();
    }

    @Override
    public Predicate<Table> get()
    {
        if (hideNonDeltaLakeTables) {
            //TODO: implement this in the spark metastore
            return DefaultGlueMetastoreTableFilterProvider::isDeltaLakeTable;
        }
        return table -> true;
    }
}

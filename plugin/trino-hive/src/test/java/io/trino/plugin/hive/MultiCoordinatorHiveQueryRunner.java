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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.plugin.postgresql.PostgreSqlPlugin;
import io.trino.plugin.postgresql.TestingPostgreSqlServer;
import io.trino.plugin.resourcegroups.ResourceGroupManagerPlugin;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SelectedRole;
import io.trino.testing.DistributedQueryRunner;
import io.trino.tpch.TpchTable;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.hive.HiveQueryRunner.HIVE_CATALOG;
import static io.trino.plugin.hive.HiveQueryRunner.TPCH_SCHEMA;
import static io.trino.plugin.hive.security.HiveSecurityModule.ALLOW_ALL;
import static io.trino.spi.security.SelectedRole.Type.ROLE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.tpch.TpchTable.CUSTOMER;
import static io.trino.tpch.TpchTable.NATION;
import static java.nio.file.Files.createDirectories;

public final class MultiCoordinatorHiveQueryRunner
{
    private static final Logger log = Logger.get(MultiCoordinatorHiveQueryRunner.class);

    private MultiCoordinatorHiveQueryRunner() {}

    public static DistributedQueryRunner create()
            throws Exception
    {
        return builder().build();
    }

    public static MultiCoordinatorHiveQueryRunner.Builder<MultiCoordinatorHiveQueryRunner.Builder<?>> builder()
    {
        return new MultiCoordinatorHiveQueryRunner.Builder<>();
    }

    public static MultiCoordinatorHiveQueryRunner.Builder<MultiCoordinatorHiveQueryRunner.Builder<?>> builder(Session defaultSession)
    {
        return new MultiCoordinatorHiveQueryRunner.Builder<>(defaultSession);
    }

    public static class Builder<SELF extends MultiCoordinatorHiveQueryRunner.Builder<?>>
            extends HiveQueryRunner.Builder<SELF>
    {
        private int coordinatorCount = 1;

        protected Builder()
        {
            this(createSession(Optional.of(new SelectedRole(ROLE, Optional.of("admin")))));
        }

        protected Builder(Session defaultSession)
        {
            super(defaultSession);
        }

        private static Session createSession(Optional<SelectedRole> role)
        {
            return testSessionBuilder()
                    .setIdentity(Identity.forUser("hive")
                            .withConnectorRoles(role.map(selectedRole -> ImmutableMap.of(HIVE_CATALOG, selectedRole))
                                    .orElse(ImmutableMap.of()))
                            .build())
                    .setCatalog(HIVE_CATALOG)
                    .setSchema(TPCH_SCHEMA)
                    .build();
        }

        @CanIgnoreReturnValue
        public SELF setCoordinatorCount(int coordinatorCount)
        {
            this.coordinatorCount = coordinatorCount;
            return self();
        }

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            setBackupCoordinatorProperties(ImmutableMap.of("http-server.http.port", "8081",
                    "primaryCoordinator", "false"));
            return super.build();
        }
    }

    public static void main(String[] args)
            throws Exception
    {
//        Logging.initialize().setRootLevel(Level.DEBUG);
        Optional<Path> baseDataDir = Optional.empty();
        if (args.length > 0) {
            if (args.length != 1) {
                System.err.println("usage: HiveQueryRunner [baseDataDir]");
                System.exit(1);
            }

            Path path = Paths.get(args[0]);
            createDirectories(path);
            baseDataDir = Optional.of(path);
        }

        DistributedQueryRunner queryRunner = MultiCoordinatorHiveQueryRunner.builder()
                .setCoordinatorCount(2)
                .setExtraProperties(ImmutableMap.of("http-server.http.port", "8080"))
                .setHiveProperties(ImmutableMap.of("hive.security", ALLOW_ALL))
                .setSkipTimezoneSetup(true)
                .setInitialTables(TpchTable.getTables())
                .setBaseDataDir(baseDataDir)
                .setTpcdsCatalogEnabled(true)
                // Uncomment to enable standard column naming (column names to be prefixed with the first letter of the table name, e.g.: o_orderkey vs orderkey)
                // and standard column types (decimals vs double for some columns). This will allow running unmodified tpch queries on the cluster.
                //.setTpchColumnNaming(ColumnNaming.STANDARD)
                //.setTpchDecimalTypeMapping(DecimalTypeMapping.DECIMAL)
                .build();
        queryRunner.installPlugin(new ResourceGroupManagerPlugin());
        queryRunner.getCoordinator().getResourceGroupManager().get().setConfigurationManager("file",
                ImmutableMap.of("resource-groups.config-file", "/Users/lukasz.stec/Library/Application Support/JetBrains/IntelliJIdea2023.3/scratches/query_investigations/multi-coordinator/local-tests/resource-groups.json"));
        queryRunner.getBackupCoordinator().get().getResourceGroupManager().get().setConfigurationManager("file",
                ImmutableMap.of("resource-groups.config-file", "/Users/lukasz.stec/Library/Application Support/JetBrains/IntelliJIdea2023.3/scratches/query_investigations/multi-coordinator/local-tests/resource-groups.json"));

        TestingPostgreSqlServer postgreSqlServer = new TestingPostgreSqlServer();

        Map<String, String> connectorProperties = new HashMap<>();
        connectorProperties.putIfAbsent("connection-url", postgreSqlServer.getJdbcUrl());
        connectorProperties.putIfAbsent("connection-user", postgreSqlServer.getUser());
        connectorProperties.putIfAbsent("connection-password", postgreSqlServer.getPassword());
        connectorProperties.putIfAbsent("postgresql.include-system-tables", "true");
        //connectorProperties.putIfAbsent("postgresql.experimental.enable-string-pushdown-with-collate", "true");

        queryRunner.installPlugin(new PostgreSqlPlugin());
        queryRunner.createCatalog("postgresql", "postgresql", connectorProperties);

        Thread.sleep(10);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n==== main", queryRunner.getCoordinator().getBaseUrl());
        log.info("\n====\n%s\n==== backup", queryRunner.getBackupCoordinator().map(TestingTrinoServer::getBaseUrl).orElseThrow());
    }
}

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
package io.trino.plugin.lakehouse;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.spi.security.Identity;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Map;

import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.SELECT_COLUMN;
import static io.trino.testing.TestingAccessControlManager.privilege;
import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that the Hive connector's {@code hive.trino-views.run-as-invoker} property is honored by
 * the Lakehouse connector, which serves views through {@code HiveMetadata}.
 */
public class TestLakehouseTrinoViewsRunAsInvoker
        extends AbstractTestQueryFramework
{
    private static final String DEFINER_CATALOG = "lakehouse";
    private static final String INVOKER_CATALOG = "lakehouse_invoker";
    private static final String SCHEMA = "views";
    private static final String VIEW_OWNER = "view_owner";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        File catalogDir = new File(createTempDirectory("test_lakehouse_trino_views").toFile(), "catalog");

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("hive.metastore", "file")
                .put("hive.metastore.catalog.dir", catalogDir.toURI().toString())
                .put("fs.hadoop.enabled", "true")
                .buildOrThrow();

        QueryRunner queryRunner = LakehouseQueryRunner.builder()
                .addLakehouseProperties(properties)
                .build();

        // same metastore and data, differing only in how Trino views are executed
        queryRunner.createCatalog(
                INVOKER_CATALOG,
                "lakehouse",
                ImmutableMap.<String, String>builder()
                        .putAll(properties)
                        .put("hive.trino-views.run-as-invoker", "true")
                        .buildOrThrow());

        queryRunner.execute("CREATE SCHEMA %s.%s".formatted(DEFINER_CATALOG, SCHEMA));
        queryRunner.execute("CREATE TABLE %s.%s.base_table (x integer) WITH (type = 'hive')".formatted(DEFINER_CATALOG, SCHEMA));
        queryRunner.execute("INSERT INTO %s.%s.base_table VALUES 1".formatted(DEFINER_CATALOG, SCHEMA));

        // views are owned by a user other than the one running the queries below
        Session ownerSession = Session.builder(queryRunner.getDefaultSession())
                .setIdentity(Identity.ofUser(VIEW_OWNER))
                .setCatalog(DEFINER_CATALOG)
                .setSchema(SCHEMA)
                .build();
        queryRunner.execute(ownerSession, "CREATE VIEW definer_view AS SELECT * FROM base_table");
        queryRunner.execute(ownerSession, "CREATE VIEW invoker_view SECURITY INVOKER AS SELECT * FROM base_table");

        return queryRunner;
    }

    @Test
    void testViewIsReportedAsRunAsInvoker()
    {
        assertThat((String) computeScalar("SHOW CREATE VIEW %s.%s.definer_view".formatted(DEFINER_CATALOG, SCHEMA)))
                .doesNotContain("SECURITY INVOKER");
        assertThat((String) computeScalar("SHOW CREATE VIEW %s.%s.definer_view".formatted(INVOKER_CATALOG, SCHEMA)))
                .contains("SECURITY INVOKER");
    }

    @Test
    void testDefinerViewUsesOwnerPrivileges()
    {
        // the view body runs as VIEW_OWNER, so denying the invoker access to the underlying table has no effect
        assertAccessAllowed(
                "SELECT * FROM %s.%s.definer_view".formatted(DEFINER_CATALOG, SCHEMA),
                privilege(getSession().getUser(), "base_table", SELECT_COLUMN));
    }

    @Test
    void testRunAsInvokerUsesInvokerPrivileges()
    {
        // the view body runs as the invoker, so denying that user access to the underlying table fails the query
        assertAccessDenied(
                "SELECT * FROM %s.%s.definer_view".formatted(INVOKER_CATALOG, SCHEMA),
                "Cannot select from columns \\[x] in table or view .*base_table.*",
                privilege(getSession().getUser(), "base_table", SELECT_COLUMN));
    }

    @Test
    void testExplicitInvokerViewIsUnaffected()
    {
        for (String catalog : new String[] {DEFINER_CATALOG, INVOKER_CATALOG}) {
            assertAccessDenied(
                    "SELECT * FROM %s.%s.invoker_view".formatted(catalog, SCHEMA),
                    "Cannot select from columns \\[x] in table or view .*base_table.*",
                    privilege(getSession().getUser(), "base_table", SELECT_COLUMN));
        }
    }
}

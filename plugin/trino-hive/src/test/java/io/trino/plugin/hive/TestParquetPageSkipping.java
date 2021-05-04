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
import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static io.trino.testing.assertions.Assert.assertEquals;
import static io.trino.tpch.TpchTable.ORDERS;
import static java.lang.String.format;
import static org.testng.Assert.assertTrue;

public class TestParquetPageSkipping
        extends AbstractTestQueryFramework
{
    private static final String PAGE_SKIPPING_TABLE = "orders_bucketed_and_sorted";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.builder()
                .setHiveProperties(ImmutableMap.of(
                        "hive.allow-register-partition-procedure", "true",
                        // Reduce writer sort buffer size to ensure SortingFileWriter gets used
                        "hive.writer-sort-buffer-size", "1MB",
                        "parquet.use-column-index", "true"))
                .setInitialTables(ImmutableList.of(ORDERS))
                .build();
    }

    private void buildSortedTables(String sortByColumnName, String sortByColumnType)
    {
        String createTableTemplate =
                "CREATE TABLE %s.%s.%s (\n" +
                "   orderkey bigint,\n" +
                "   custkey bigint,\n" +
                "   orderstatus varchar(1),\n" +
                "   totalprice double,\n" +
                "   orderdate date,\n" +
                "   orderpriority varchar(15),\n" +
                "   clerk varchar(15),\n" +
                "   shippriority integer,\n" +
                "   comment varchar(79),\n" +
                "   rvalues double array\n" +
                ")\n" +
                "WITH (\n" +
                "   format = 'PARQUET',\n" +
                "   bucketed_by = array['orderstatus'],\n" +
                "   bucket_count = 1,\n" +
                "   sorted_by = array['%s']\n" +
                ")";
        createTableTemplate = createTableTemplate.replaceFirst(sortByColumnName + "[ ]+([^,]*)", sortByColumnName + " " + sortByColumnType);
        String createTableSql = format(
                createTableTemplate,
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                PAGE_SKIPPING_TABLE,
                sortByColumnName);

        assertUpdate(createTableSql);
        assertUpdate(
                Session.builder(getSession())
                        .setCatalogSessionProperty(getSession().getCatalog().get(), "parquet_writer_page_size", "10000B")
                        .setCatalogSessionProperty(getSession().getCatalog().get(), "parquet_writer_block_size", "100GB")
                        .build(),
                format("INSERT INTO %s SELECT *,ARRAY[rand(),rand(),rand()] FROM orders", PAGE_SKIPPING_TABLE),
                15000);
    }

    @Test(dataProvider = "dataType")
    public void testPageSkipping(String sortByColumn, String sortByColumnType, Object lowValue, Object middleLowValue, Object middleHighValue, Object highValue)
    {
        try {
            buildSortedTables(sortByColumn, sortByColumnType);
            assertSameResults(format("SELECT %s FROM %s WHERE %s = %s", sortByColumn, PAGE_SKIPPING_TABLE, sortByColumn, middleLowValue));
            assertSameResults(format("SELECT %s FROM %s WHERE %s < %s ORDER BY %s", sortByColumn, PAGE_SKIPPING_TABLE, sortByColumn, lowValue, sortByColumn));
            assertSameResults(format("SELECT %s FROM %s WHERE %s > %s ORDER BY %s", sortByColumn, PAGE_SKIPPING_TABLE, sortByColumn, highValue, sortByColumn));
            assertSameResults(format("SELECT %s FROM %s WHERE %s BETWEEN %s AND %s ORDER BY %s", sortByColumn, PAGE_SKIPPING_TABLE, sortByColumn, middleLowValue, middleHighValue, sortByColumn));
            // Tests synchronization of reading values across columns
            assertSameResults(format("SELECT * FROM %s WHERE %s = %s ORDER BY orderkey", PAGE_SKIPPING_TABLE, sortByColumn, middleLowValue));
            assertSameResults(format("SELECT * FROM %s WHERE %s < %s ORDER BY orderkey", PAGE_SKIPPING_TABLE, sortByColumn, lowValue));
            assertSameResults(format("SELECT * FROM %s WHERE %s > %s ORDER BY orderkey", PAGE_SKIPPING_TABLE, sortByColumn, highValue));
            assertSameResults(format("SELECT * FROM %s WHERE %s BETWEEN %s AND %s ORDER BY orderkey", PAGE_SKIPPING_TABLE, sortByColumn, middleLowValue, middleHighValue));
        }
        finally {
            assertUpdate(format("DROP TABLE IF EXISTS %s", PAGE_SKIPPING_TABLE));
        }
    }

    private void assertSameResults(String query)
    {
        MaterializedResult withColumnIndexing = computeActual(query);
        MaterializedResult withoutColumnIndexing = computeActual(Session.builder(getSession())
                        .setCatalogSessionProperty(getSession().getCatalog().get(), "parquet_use_column_index", "false")
                        .build(),
                query);
        assertTrue(withoutColumnIndexing.getRowCount() > 0);
        assertEquals(withColumnIndexing, withoutColumnIndexing);
    }

    @DataProvider
    public Object[][] dataType()
    {
        return new Object[][] {
                {"orderkey", "bigint", 2, 7520, 7523, 14950},
                {"totalprice", "double", 974.04, 131094.34, 131279.97, 406938.36},
                {"totalprice", "real", 974.04, 131094.34, 131279.97, 406938.36},
                {"orderdate", "date", "DATE '1992-01-05'", "DATE '1995-10-13'", "DATE '1995-10-13'", "DATE '1998-07-29'"},
                {"orderdate", "timestamp", "TIMESTAMP '1992-01-05'", "TIMESTAMP '1995-10-13'", "TIMESTAMP '1995-10-14'", "TIMESTAMP '1998-07-29'"},
                {"clerk", "varchar(15)", "'Clerk#000000006'", "'Clerk#000000508'", "'Clerk#000000513'", "'Clerk#000000996'"},
                {"custkey", "integer", 4, 634, 640, 1493},
                {"custkey", "smallint", 4, 634, 640, 1493}
        };
    }
}

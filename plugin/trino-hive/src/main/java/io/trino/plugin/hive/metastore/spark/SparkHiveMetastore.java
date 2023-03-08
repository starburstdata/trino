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

package io.trino.plugin.hive.metastore.spark;

import io.airlift.log.Logger;
import io.trino.plugin.hive.HiveColumnStatisticType;
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.PartitionStatistics;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HivePrincipal;
import io.trino.plugin.hive.metastore.HivePrivilegeInfo;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.PartitionWithStatistics;
import io.trino.plugin.hive.metastore.PrincipalPrivileges;
import io.trino.plugin.hive.metastore.Storage;
import io.trino.plugin.hive.metastore.StorageFormat;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil;
import io.trino.spi.TrinoException;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.type.Type;
import org.apache.hive.jdbc.HiveDriver;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static io.trino.plugin.hive.metastore.StorageFormat.create;
import static io.trino.plugin.hive.util.HiveUtil.makePartName;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;

public class SparkHiveMetastore
        implements HiveMetastore
{
    private static final Logger log = Logger.get(SparkHiveMetastore.class);
    SparkHiveMetastoreConfig config;
    Connection connection;

    public SparkHiveMetastore(SparkHiveMetastoreConfig config)
    {
        this.config = config;
        this.connection = establishConnection();
    }

    private Connection establishConnection()
    {
        try {
            Driver driver = getDriver(config.getIsDatabricks());
            Properties properties = new Properties();
            properties.setProperty("user", config.getUser());
            if (config.getPassword().isPresent()) {
                properties.setProperty("password", config.getPassword().get());
            }
            return driver.connect(config.getHiveJdbcUrl(), properties);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private Driver getDriver(Boolean isDatabricks)
    {
        /*if (isDatabricks) {
            return new com.databricks.client.jdbc.Driver();
        }*/
        //TODO: investigate if needed, fix deps for Databricks driver
        return new HiveDriver();
    }

    private ResultSet executeSql(String sql)
    {
        try {
            Statement statement = connection.createStatement();
            log.debug("Submitting sql: " + sql);
            ResultSet resultSet = statement.executeQuery(sql);
            return resultSet;
        }
        catch (SQLException e) {
            connection = establishConnection();

            throw new RuntimeException(e);
        }
    }

    private ResultSet executeSqlCatchable(String sql)
            throws SQLException
    {
        Statement statement = connection.createStatement();
        log.debug("Submitting sql: " + sql);
        ResultSet resultSet = statement.executeQuery(sql);
        return resultSet;
    }

    private <K, V> Map<K, V> resultSetToMap(ResultSet resultSet, int keyIndex, int valueIndex, K keyType, V valueType)
    {
        try {
            Map<K, V> resultMap = new HashMap<>();
            while (resultSet.next()) {
                resultMap.put((K) resultSet.getObject(keyIndex, keyType.getClass()), (V) resultSet.getObject(valueIndex, valueType.getClass()));
            }
            resultSet.close();
            return resultMap;
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private Map<String, String> resultSetToMap(ResultSet resultSet, int keyIndex, int valueIndex)
    {
        try {
            Map<String, String> resultMap = new HashMap<>();
            while (resultSet.next()) {
                resultMap.put(resultSet.getString(keyIndex), resultSet.getString(valueIndex));
            }
            resultSet.close();
            return resultMap;
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private <T> List<T> resultSetToList(ResultSet resultSet, int index, T dummyType)
    {
        try {
            List<T> resultList = new ArrayList<>();
            while (resultSet.next()) {
                resultList.add((T) resultSet.getObject(index, dummyType.getClass()));
            }
            resultSet.close();
            return resultList;
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private List<String> resultSetToList(ResultSet resultSet, int index)
    {
        try {
            List<String> resultList = new ArrayList<>();
            while (resultSet.next()) {
                resultList.add(resultSet.getString(index));
            }
            resultSet.close();
            return resultList;
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Optional<Database> getDatabase(String databaseName)
    {
        String nameProperty = "namespace name";
        String locationProperty = "location";
        String ownerProperty = "owner";
        String commentProperty = "comment";
        Map<String, String> databaseProperties =
                resultSetToMap(executeSql(String.format("DESCRIBE SCHEMA %s", databaseName)), 1, 2);
        return Optional.of(new Database(
                databaseProperties.get(nameProperty),
                Optional.of(databaseProperties.get(locationProperty)),
                Optional.of(databaseProperties.get(ownerProperty)),
                Optional.of(PrincipalType.USER),
                Optional.of(databaseProperties.get(commentProperty)),
                Collections.emptyMap())); //TODO: investigate Database parameters
    }

    @Override
    public List<String> getAllDatabases()
    {
        return resultSetToList(executeSql("SHOW SCHEMAS"), 1);
    }

    class DescribeExtendedResult
    {
        //TODO: should i make these case insensitive?
        public static String locationProperty = "Location";
        public static String ownerProperty = "Owner";
        public static String typeProperty = "Type";
        public static String commentProperty = "Comment";
        public static String serdeProperty = "Serde Library";
        public static String inputFormatProperty = "InputFormat";
        public static String outputFormatProperty = "OutputFormat";

        public Map<String, String> getResultMap()
        {
            return resultMap;
        }

        public void setResultMap(Map<String, String> resultMap)
        {
            this.resultMap = resultMap;
        }

        public List<Column> getDataColumns()
        {
            return dataColumns;
        }

        public void setDataColumns(List<Column> dataColumns)
        {
            this.dataColumns = dataColumns;
        }

        public List<Column> getPartitionColumns()
        {
            return partitionColumns;
        }

        public void setPartitionColumns(List<Column> partitionColumns)
        {
            this.partitionColumns = partitionColumns;
        }

        Map<String, String> resultMap;
        List<Column> dataColumns;
        List<Column> partitionColumns;

        public Optional<Storage> getStorage()
        {
            return storage;
        }

        public void setStorage(Storage storage)
        {
            this.storage = Optional.of(storage);
        }

        Optional<Storage> storage;

        public Map<String, String> getParameters()
        {
            return parameters;
        }

        public void setParameters(Map<String, String> parameters)
        {
            this.parameters = parameters;
        }

        Map<String, String> parameters;

        public DescribeExtendedResult()
        {
            resultMap = new HashMap<>();
            dataColumns = new ArrayList<>();
            partitionColumns = new ArrayList<>();
            storage = Optional.empty();
            parameters = Collections.emptyMap(); //TODO: populate parameters
        }
    }

    private DescribeExtendedResult parseDscribeExtended(ResultSet describeExtended)
    {
        String partitionRowDelimiter = "# Partition Information";
        String tableInfoDelimiter = "# Detailed Table Information";
        String partitionInfoDelimiter = "# Detailed Partition Information";
        String storageInfoDelimiter = "# Storage Information";
        enum RowBlockState
        {
            DATACOLUMNS,
            PARTCOLUMNS,
            TABLEINFO,
            STORAGEINFO
        }

        DescribeExtendedResult describeExtendedResult = new DescribeExtendedResult();
        Map<String, String> resultMap = describeExtendedResult.getResultMap();
        List<Column> dataColumns = describeExtendedResult.getDataColumns();
        List<Column> partitionColumns = describeExtendedResult.getPartitionColumns();

        RowBlockState rowBlock = RowBlockState.DATACOLUMNS;
        try {
            while (describeExtended.next()) {
                if (describeExtended.getString(1).equals(partitionRowDelimiter)) {
                    rowBlock = RowBlockState.PARTCOLUMNS;
                    continue;
                }
                if (describeExtended.getString(1).equals(tableInfoDelimiter) || describeExtended.getString(1).equals(partitionInfoDelimiter)) {
                    rowBlock = RowBlockState.TABLEINFO;
                    continue;
                }
                if (describeExtended.getString(1).equals(storageInfoDelimiter)) {
                    rowBlock = RowBlockState.STORAGEINFO;
                    continue;
                }
                if (describeExtended.getString(1).startsWith("#") || describeExtended.getString(1).isBlank()) {
                    continue;
                }

                switch (rowBlock) {
                    case DATACOLUMNS:
                        dataColumns.add(new Column(
                                describeExtended.getString(1),
                                HiveType.valueOf(describeExtended.getString(2)),
                                Optional.ofNullable(describeExtended.getString(3))));
                        break;
                    case PARTCOLUMNS:
                        partitionColumns.add(new Column(
                                describeExtended.getString(1),
                                HiveType.valueOf(describeExtended.getString(2)),
                                Optional.ofNullable(describeExtended.getString(3))));
                        break;
                    case TABLEINFO:
                        resultMap.put(describeExtended.getString(1), describeExtended.getString(2));
                        break;
                    case STORAGEINFO:
                    default:
                        continue;
                }
            }

            describeExtended.close();
            dataColumns.removeAll(partitionColumns);

            if (resultMap.containsKey(DescribeExtendedResult.locationProperty)) {
                StorageFormat storageFormat = create(resultMap.get(DescribeExtendedResult.serdeProperty), resultMap.get(DescribeExtendedResult.inputFormatProperty), resultMap.get(DescribeExtendedResult.outputFormatProperty));
                describeExtendedResult.setStorage(new Storage(
                        storageFormat,
                        Optional.ofNullable(resultMap.get(DescribeExtendedResult.locationProperty)),
                        Optional.empty(), //Spark bucketing is not compatible, alway treat as unbucketed
                        false,
                        Collections.emptyMap())); //TODO: investigate if Storage Properties is equivalent to our SerdeProperties
            }
            return describeExtendedResult;
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Optional<Table> getTable(String databaseName, String tableName)
    {
        DescribeExtendedResult describeExtendedResult = parseDscribeExtended(executeSql(String.format("describe table extended %s.%s", databaseName, tableName)));
        Map<String, String> resultMap = describeExtendedResult.getResultMap();
        List<Column> dataColumns = describeExtendedResult.getDataColumns();
        List<Column> partitionColumns = describeExtendedResult.getPartitionColumns();

        if (describeExtendedResult.getStorage().isEmpty()) {
            throw new TrinoException(HIVE_METASTORE_ERROR, String.format("Location unavailable for %s.%s", databaseName, tableName));
        }
        Storage storage = describeExtendedResult.getStorage().get();
        return Optional.of(new Table(
                databaseName,
                tableName,
                Optional.of(resultMap.get(DescribeExtendedResult.ownerProperty)),
                resultMap.get(DescribeExtendedResult.typeProperty).equals("MANAGED") ?
                        "MANAGED_TABLE" : "EXTERNAL",
                storage,
                dataColumns,
                partitionColumns,
                describeExtendedResult.getParameters(),
                Optional.empty(), //TODO: investigate view support
                Optional.empty(),
                OptionalLong.empty()));
    }

    @Override
    public Set<HiveColumnStatisticType> getSupportedColumnStatistics(Type type)
    {
        return ThriftMetastoreUtil.getSupportedColumnStatistics(type);
    }

    @Override
    public PartitionStatistics getTableStatistics(Table table)
    {
        return PartitionStatistics.empty();
    }

    @Override
    public Map<String, PartitionStatistics> getPartitionStatistics(Table table, List<Partition> partitions)
    {
        Map<String, PartitionStatistics> partitionStatistics = new HashMap<>();
        for (Partition partition : partitions) {
            partitionStatistics.put(
                    makePartName(table.getPartitionColumns().stream().map(Column::getName).collect(toImmutableList()),
                    partition.getValues()), PartitionStatistics.empty());
        }
        return partitionStatistics;
    }

    @Override
    public void updateTableStatistics(String databaseName, String tableName, AcidTransaction transaction, Function<PartitionStatistics, PartitionStatistics> update)
    {
    }

    @Override
    public void updatePartitionStatistics(Table table, Map<String, Function<PartitionStatistics, PartitionStatistics>> updates)
    {
    }

    @Override
    public List<String> getAllTables(String databaseName)
    {
        return resultSetToList(executeSql(String.format("show tables from %s", databaseName)), 2);
    }

    @Override
    public List<String> getTablesWithParameter(String databaseName, String parameterKey, String parameterValue)
    {
        return getAllTables(databaseName).stream()
            .filter(tableName -> {
                // TODO: this will perform terribly
                Optional<Table> table = getTable(databaseName, tableName);
                if (table.isEmpty()) {
                    return false;
                }
                String value = table.get().getParameters().get(parameterKey);
                return value != null && value.equals(parameterValue);
            })
            .collect(Collectors.toList());
    }

    @Override
    public List<String> getAllViews(String databaseName)
    {
        return Collections.emptyList();
        //TODO: filter views and tables
    }

    @Override
    public void createDatabase(Database database)
    {
        if (database.getLocation().isPresent()) {
            executeSql(String.format("CREATE SCHEMA %s LOCATION %s", database.getDatabaseName(), database.getLocation().get()));
        }
        else {
            executeSql(String.format("CREATE SCHEMA %s", database.getDatabaseName()));
        }
    }

    @Override
    public void dropDatabase(String databaseName, boolean deleteData)
    {
        if (!deleteData) {
            throw new TrinoException(NOT_SUPPORTED, "dropDatabase must delete database directory");
        }
        executeSql(String.format("DROP SCHEMA %s CASCADE", databaseName));
        //TODO: ensure that CASCADE semantics (deletes all tables) match those of Trino drop schema
    }

    @Override
    public void renameDatabase(String databaseName, String newDatabaseName)
    {
    }

    @Override
    public void setDatabaseOwner(String databaseName, HivePrincipal principal)
    {
    }

    @Override
    public void createTable(Table table, PrincipalPrivileges principalPrivileges)
    {
        executeSql(String.format("CREATE TABLE %s.%s USING %s (%s) %s %s",
                table.getSchemaTableName().getSchemaName(),
                table.getSchemaTableName().getTableName(),
                table.getTableType(), //TODO: should we restrict this to a subset of available formats?
                buildSchema(table.getDataColumns(), table.getPartitionColumns()),
                buildPartitionedBy(table.getPartitionColumns()),
                buildLocation(table)));
        //TODO: add handling for other table properties
    }

    String buildSchema(List<Column> dataColumns, List<Column> partitionColumns)
    {
        return String.join(",", dataColumns.stream().map(column -> column.getName() + " " + column.getType()).collect(Collectors.toList()))
                + (partitionColumns.size() > 0 ? "," : "")
                + String.join(",", partitionColumns.stream().map(column -> column.getName() + " " + column.getType()).collect(Collectors.toList()));
    }

    String buildPartitionedBy(List<Column> partitionColumns)
    {
        if (partitionColumns.size() == 0) {
            return "";
        }
        return "PARTITIONED BY (" + String.join(",", partitionColumns.stream().map(Column::getName).collect(Collectors.toList())) + ",";
    }

    String buildLocation(Table table)
    {
        if (table.getParameters().containsKey("location")) {
            return "LOCATION " + table.getParameters().get("location");
        }
        return "";
    }

    @Override
    public void dropTable(String databaseName, String tableName, boolean deleteData)
    {
        executeSql(String.format("DROP TABLE %s.%s", databaseName, tableName));
    }

    @Override
    public void replaceTable(String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges)
    {
    }

    @Override
    public void renameTable(String databaseName, String tableName, String newDatabaseName, String newTableName)
    {
        executeSql(String.format("ALTER TABLE RENAME %s.%s TO %S.%s", databaseName, tableName, newDatabaseName, newTableName));
        //TODO: check this syntax is valid for spark
    }

    @Override
    public void commentTable(String databaseName, String tableName, Optional<String> comment)
    {
    }

    @Override
    public void setTableOwner(String databaseName, String tableName, HivePrincipal principal)
    {
    }

    @Override
    public void commentColumn(String databaseName, String tableName, String columnName, Optional<String> comment)
    {
    }

    @Override
    public void addColumn(String databaseName, String tableName, String columnName, HiveType columnType, String columnComment)
    {
    }

    @Override
    public void renameColumn(String databaseName, String tableName, String oldColumnName, String newColumnName)
    {
    }

    @Override
    public void dropColumn(String databaseName, String tableName, String columnName)
    {
    }

    private String buildPartitionSpec(Table table, List<String> partitionValues)
    {
        if (partitionValues.size() != table.getPartitionColumns().size()) {
            throw new TrinoException(HIVE_METASTORE_ERROR,
                    String.format("Partition values size %s does not mathc part cols size %s", partitionValues.size(), table.getPartitionColumns().size()));
        }

        StringBuilder partitionSpec = new StringBuilder();
        for (int i = 0; i < table.getPartitionColumns().size(); i++) {
            if (i > 0) {
                partitionSpec.append(",");
            }
            partitionSpec.append(String.format("%s='%s'", table.getPartitionColumns().get(i).getName(), partitionValues.get(i)));
        }
        return partitionSpec.toString();
    }

    @Override
    public Optional<Partition> getPartition(Table table, List<String> partitionValues)
    {
        String partitionSpec = buildPartitionSpec(table, partitionValues);
        try {
            DescribeExtendedResult describeExtendedResult = parseDscribeExtended(executeSqlCatchable(String.format("describe extended %s.%s partition (%s)",
                    table.getDatabaseName(), table.getTableName(), partitionSpec)));
            return Optional.of(new Partition(table.getDatabaseName(),
                    table.getTableName(),
                    partitionValues,
                    describeExtendedResult.getStorage().get(),
                    describeExtendedResult.getDataColumns(),
                    describeExtendedResult.getParameters()));
        }
        catch (SQLException e) {
            if (e.getMessage().contains("Partition not found")) {
                return Optional.empty();
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public Optional<List<String>> getPartitionNamesByFilter(String databaseName, String tableName, List<String> columnNames, TupleDomain<String> partitionKeysFilter)
    {
        return Optional.of(resultSetToList(executeSql(String.format("SHOW PARTITIONS %s.%s", databaseName, tableName)), 1));
        //TODO: translate filter into a hive-style partition spec
    }

    @Override
    public Map<String, Optional<Partition>> getPartitionsByNames(Table table, List<String> partitionNames)
    {
        String partitionDelimiter = "/";
        Map<String, Optional<Partition>> partitions = new HashMap<>();
        for (String partitionName : partitionNames) {
            partitions.put(partitionName, getPartition(table,
                    Arrays.stream(partitionName.split(partitionDelimiter)).map(kv -> kv.split("=")[1]).collect(Collectors.toList())));
        }
        return partitions;
       //TODO: look at perf impact. May need a spark metadata REST server after all if there is no batch retrieval
    }

    @Override
    public void addPartitions(String databaseName, String tableName, List<PartitionWithStatistics> partitions)
    {
        for (PartitionWithStatistics partition : partitions) {
            addPartition(databaseName, tableName, partition.getPartition());
        }
    }
    //TODO: change to bulk using list of partition specs:
    // ALTER TABLE table_identifier ADD [IF NOT EXISTS]
    //    ( partition_spec [ partition_spec ... ]
    private void addPartition(String databaseName, String tableName, Partition partition)
    {
        Optional<Table> table = getTable(databaseName, tableName); // caching is important..
        if (table.isEmpty()) {
            throw new TrinoException(HIVE_METASTORE_ERROR, String.format("Table does not exist %s.%s", databaseName, tableName));
        }
        String partitionSpec = buildPartitionSpec(table.get(), partition.getValues());
        executeSql(String.format("ALTER TABLE %s.%s ADD IF NOT EXISTS PARTITION (%s)", databaseName, tableName, partitionSpec));
    }

    @Override
    public void dropPartition(String databaseName, String tableName, List<String> parts, boolean deleteData)
    {
    }

    @Override
    public void alterPartition(String databaseName, String tableName, PartitionWithStatistics partition)
    {
    }

    @Override
    public void createRole(String role, String grantor)
    {
    }

    @Override
    public void dropRole(String role)
    {
    }

    @Override
    public Set<String> listRoles()
    {
        return null;
    }

    @Override
    public void grantRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
    {
    }

    @Override
    public void revokeRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
    {
    }

    @Override
    public Set<RoleGrant> listGrantedPrincipals(String role)
    {
        return null;
    }

    @Override
    public Set<RoleGrant> listRoleGrants(HivePrincipal principal)
    {
        return null;
    }

    @Override
    public void grantTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilegeInfo.HivePrivilege> privileges, boolean grantOption)
    {
    }

    @Override
    public void revokeTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilegeInfo.HivePrivilege> privileges, boolean grantOption)
    {
    }

    @Override
    public Set<HivePrivilegeInfo> listTablePrivileges(String databaseName, String tableName, Optional<String> tableOwner, Optional<HivePrincipal> principal)
    {
        return null;
    }
}

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
import static io.trino.plugin.hive.HiveType.HIVE_DATE;
import static io.trino.plugin.hive.HiveType.HIVE_INT;
import static io.trino.plugin.hive.HiveType.HIVE_STRING;
import static io.trino.plugin.hive.metastore.StorageFormat.create;
import static io.trino.plugin.hive.util.HiveUtil.makePartName;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Locale.ENGLISH;

public class SparkHiveMetastore
        implements HiveMetastore
{
    Connection connection;

    public SparkHiveMetastore(SparkHiveMetastoreConfig config)
    {
        try {
            Driver driver = getDriver(config.getIsDatabricks());
            Properties properties = new Properties();
            properties.setProperty("user", config.getUser());
            if (config.getPassword().isPresent()) {
                properties.setProperty("password", config.getPassword().get());
            }
            connection = driver.connect(config.getHiveJdbcUrl(), properties);
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
        return new HiveDriver();
    }

    private ResultSet executeSql(String sql)
    {
        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            return resultSet;
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
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
                new HashMap<String, String>()));
    }

    @Override
    public List<String> getAllDatabases()
    {
        return resultSetToList(executeSql("SHOW SCHEMAS"), 1);
    }

    private HiveType nameToHiveType(String typeName)
    {
        if (typeName.toLowerCase(ENGLISH).equals("int")) {
            return HIVE_INT;
        }
        else if (typeName.toLowerCase(ENGLISH).equals("string")) {
            return HIVE_STRING;
        }
        else if (typeName.toLowerCase(ENGLISH).equals("date")) {
            return HIVE_DATE;
        }
        return null;
        //TODO: fill in the rest of these
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
        enum RowBlockState
        {
            DATACOLUMNS,
            PARTCOLUMNS,
            TABLEINFO
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
                if (describeExtended.getString(1).startsWith("#") || describeExtended.getString(1).isBlank()) {
                    continue;
                }
                switch (rowBlock) {
                    case DATACOLUMNS:
                        dataColumns.add(new Column(
                                describeExtended.getString(1),
                                nameToHiveType(describeExtended.getString(2)),
                                Optional.ofNullable(describeExtended.getString(3))));
                        break;
                    case PARTCOLUMNS:
                        partitionColumns.add(new Column(
                                describeExtended.getString(1),
                                nameToHiveType(describeExtended.getString(2)),
                                Optional.ofNullable(describeExtended.getString(3))));
                        break;
                    case TABLEINFO:
                    default:
                        resultMap.put(describeExtended.getString(1), describeExtended.getString(2));
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
                resultMap.get(DescribeExtendedResult.typeProperty),
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
        return resultSetToList(executeSql("show tables from default"), 2);
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
        executeSql(String.format("CREATE SCHEMA %s LOCATION %s", database.getDatabaseName(), database.getLocation()));
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
    }

    @Override
    public void dropTable(String databaseName, String tableName, boolean deleteData)
    {
    }

    @Override
    public void replaceTable(String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges)
    {
    }

    @Override
    public void renameTable(String databaseName, String tableName, String newDatabaseName, String newTableName)
    {
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

    @Override
    public Optional<Partition> getPartition(Table table, List<String> partitionValues)
    {
        if (table.getPartitionColumns().size() != partitionValues.size()) {
            throw new TrinoException(INVALID_ARGUMENTS, "partition values must match partition columns");
        }

        StringBuilder partitionSpec = new StringBuilder();
        for (int i = 0; i < table.getPartitionColumns().size(); i++) {
            if (i > 0) {
                partitionSpec.append(",");
            }
            partitionSpec.append(String.format("%s='%s'", table.getPartitionColumns().get(i).getName(), partitionValues.get(i)));
        }
        DescribeExtendedResult describeExtendedResult = parseDscribeExtended(executeSql(String.format("describe extended %s partition (%s)", table.getTableName(), partitionSpec.toString())));
        return Optional.of(new Partition(table.getDatabaseName(),
                table.getTableName(),
                partitionValues,
                describeExtendedResult.getStorage().get(),
                describeExtendedResult.getDataColumns(),
                describeExtendedResult.getParameters()));
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
        Map<String, Optional<Partition>> partitions = new HashMap<>();
        for (String partitionName : partitionNames) {
            partitions.put(partitionName, getPartition(table,
                    Arrays.stream(partitionName.split(",")).map(kv -> kv.split("=")[1]).collect(Collectors.toList())));
        }
        return partitions;
       //TODO: look at perf impact. May need a spark metadata REST server after all if there is no batch retrieval
    }

    @Override
    public void addPartitions(String databaseName, String tableName, List<PartitionWithStatistics> partitions)
    {
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

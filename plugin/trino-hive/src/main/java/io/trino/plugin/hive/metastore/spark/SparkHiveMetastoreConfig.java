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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import java.util.Optional;

public class SparkHiveMetastoreConfig
{
    private String hiveJdbcUrl;

    private String user;
    private Optional<String> password = Optional.empty();

    private boolean isDatabricks;

    public String getHiveJdbcUrl()
    {
        return hiveJdbcUrl;
    }

    @Config("hive.spark-metastore.jdbc.url")
    @ConfigDescription("URL of thrift server for Spark SQL client")
    public SparkHiveMetastoreConfig setHiveJdbcUrl(String hiveJdbcUrl)
    {
        this.hiveJdbcUrl = hiveJdbcUrl;
        return this;
    }

    public String getUser()
    {
        return user;
    }

    @Config("hive.spark-metastore.user")
    @ConfigDescription("Username for Spark SQL client")
    public SparkHiveMetastoreConfig setUser(String user)
    {
        this.user = user;
        return this;
    }

    public Optional<String> getPassword()
    {
        return password;
    }

    @Config("hive.spark-metastore.password")
    @ConfigDescription("Username for Spark SQL client")
    public SparkHiveMetastoreConfig setPassword(String password)
    {
        this.password = Optional.of(password);
        return this;
    }

    public boolean getIsDatabricks()
    {
        return isDatabricks;
    }

    @Config("hive.spark-metastore.databricks-enabled")
    @ConfigDescription("Set if connecting to a databricks cluster. Ensure JDBC url is correct.")
    public SparkHiveMetastoreConfig setIsDatabricks(Boolean isDatabricks)
    {
        this.isDatabricks = isDatabricks;
        return this;
    }
}

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

import com.google.common.collect.ImmutableList;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import javax.inject.Inject;

import java.util.List;

import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.integerProperty;

public class CacheSessionProperties
        implements SessionPropertiesProvider
{
    public static final String CACHE_ENABLED = "cache_enabled";
    public static final String CACHE_COMPRESSION_ENABLED = "cache_compression_enabled";
    public static final String CACHE_USE_RAW_FORMAT = "cache_use_raw_format";
    public static final String CACHE_MERGE_PAGES = "cache_merge_pages";
    public static final String CACHE_MAX_VERSIONS = "cache_max_versions";

    private final List<PropertyMetadata<?>> properties;

    @Inject
    public CacheSessionProperties(CacheConfig config)
    {
        properties = ImmutableList.of(
                booleanProperty(
                        CACHE_ENABLED,
                        "Enable cache",
                        config.isCacheEnabled(),
                        false),
                booleanProperty(
                        CACHE_COMPRESSION_ENABLED,
                        "Compression enabled",
                        config.isCompressionEnabled(),
                        false),
                booleanProperty(
                        CACHE_USE_RAW_FORMAT,
                        "Use raw format",
                        config.isUseRawFormat(),
                        false),
                booleanProperty(
                        CACHE_MERGE_PAGES,
                        "Merge pages",
                        config.isMergePages(),
                        false),
                integerProperty(
                        CACHE_MAX_VERSIONS,
                        "Max versions",
                        config.getMaxVersions(),
                        false));
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return properties;
    }

    public static boolean isCacheEnabled(ConnectorSession session)
    {
        return session.getProperty(CACHE_ENABLED, Boolean.class);
    }

    public static boolean isCacheCompressionEnabled(ConnectorSession session)
    {
        return session.getProperty(CACHE_COMPRESSION_ENABLED, Boolean.class);
    }

    public static boolean isCacheUseRawFormat(ConnectorSession session)
    {
        return session.getProperty(CACHE_USE_RAW_FORMAT, Boolean.class);
    }

    public static boolean isCacheMergePages(ConnectorSession session)
    {
        return session.getProperty(CACHE_MERGE_PAGES, Boolean.class);
    }

    public static int getCacheMaxVersions(ConnectorSession session)
    {
        return session.getProperty(CACHE_MAX_VERSIONS, Integer.class);
    }
}

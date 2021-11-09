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

import io.airlift.configuration.Config;

import java.nio.file.Path;
import java.util.Optional;

public class CacheConfig
{
    private boolean cacheEnabled = true;
    private boolean compressionEnabled = true;
    private boolean useRawFormat;
    private boolean mergePages;
    private int maxVersions = 3;
    private Optional<Path> cachePath = Optional.empty();

    public boolean isCacheEnabled()
    {
        return cacheEnabled;
    }

    @Config("cache.enabled")
    public CacheConfig setCacheEnabled(boolean cacheEnabled)
    {
        this.cacheEnabled = cacheEnabled;
        return this;
    }

    public Optional<Path> getCachePath()
    {
        return cachePath;
    }

    @Config("cache.path")
    public CacheConfig setCachePath(Path cachePath)
    {
        this.cachePath = Optional.ofNullable(cachePath);
        return this;
    }

    public boolean isCompressionEnabled()
    {
        return compressionEnabled;
    }

    @Config("cache.compression.enabled")
    public CacheConfig setCompressionEnabled(boolean compressionEnabled)
    {
        this.compressionEnabled = compressionEnabled;
        return this;
    }

    public boolean isUseRawFormat()
    {
        return useRawFormat;
    }

    @Config("cache.use-raw-format")
    public CacheConfig setUseRawFormat(boolean useRawFormat)
    {
        this.useRawFormat = useRawFormat;
        return this;
    }

    public boolean isMergePages()
    {
        return mergePages;
    }

    @Config("cache.merge-pages")
    public CacheConfig setMergePages(boolean mergePages)
    {
        this.mergePages = mergePages;
        return this;
    }

    public int getMaxVersions()
    {
        return maxVersions;
    }

    @Config("cache.max-versions")
    public CacheConfig setMaxVersions(int maxVersions)
    {
        this.maxVersions = maxVersions;
        return this;
    }

    /*
    @AssertTrue(message = "cache.path must be provided when cache.enabled is true")
    public boolean isPathProvided()
    {
        return !isCacheEnabled() || getCachePath().isPresent();
    }*/
}

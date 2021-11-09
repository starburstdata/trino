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

import io.airlift.slice.BasicSliceInput;
import io.trino.plugin.hive.cache.WorkerCacheManager.CachedData;
import io.trino.plugin.hive.cache.serde.PagesSerdeFactory;
import io.trino.plugin.hive.cache.serde.PagesSerdeUtil;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSource;

import java.io.IOException;
import java.util.Iterator;

public class CachePageSource
        implements ConnectorPageSource
{
    private final Iterator<Page> cachedPages;

    public CachePageSource(PagesSerdeFactory pagesSerdeFactory, CachedData cachedData)
    {
        if (cachedData.getPages().isPresent()) {
            cachedPages = cachedData.getPages().get().iterator();
        }
        else {
            cachedPages = PagesSerdeUtil.readPages(
                    pagesSerdeFactory.createPagesSerde(cachedData.isCompressed()),
                    new BasicSliceInput(cachedData.getSlice().get()));
        }
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public boolean isFinished()
    {
        return !cachedPages.hasNext();
    }

    @Override
    public Page getNextPage()
    {
        if (isFinished()) {
            return null;
        }

        return cachedPages.next();
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close()
            throws IOException
    {
    }
}

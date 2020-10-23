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
package io.prestosql.orc;

import io.airlift.slice.Slice;
import io.prestosql.orc.stream.OrcDataReader;

import java.io.IOException;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class UnclosableOrcDataSource
        implements OrcDataSource
{
    private final OrcDataSource delegate;

    public UnclosableOrcDataSource(OrcDataSource delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public OrcDataSourceId getId()
    {
        return delegate.getId();
    }

    @Override
    public long getReadBytes()
    {
        return delegate.getReadBytes();
    }

    @Override
    public long getReadTimeNanos()
    {
        return delegate.getReadTimeNanos();
    }

    @Override
    public long getEstimatedSize()
    {
        return delegate.getEstimatedSize();
    }

    @Override
    public long getRetainedSize()
    {
        return delegate.getRetainedSize();
    }

    @Override
    public Slice readTail(int length)
            throws IOException
    {
        return delegate.readTail(length);
    }

    @Override
    public Slice readFully(long position, int length)
            throws IOException
    {
        return delegate.readFully(position, length);
    }

    @Override
    public <K> Map<K, OrcDataReader> readFully(Map<K, DiskRange> diskRanges)
            throws IOException
    {
        return delegate.readFully(diskRanges);
    }

    @Override
    public void close()
    {
        // do not close
    }
}

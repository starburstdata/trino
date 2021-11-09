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

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class SplitSignature
{
    private final String path;
    private final long start;
    private final long length;
    private final long fileModifiedTime;

    public SplitSignature(String path, long start, long length, long fileModifiedTime)
    {
        this.path = requireNonNull(path, "path is null");
        this.start = start;
        this.length = length;
        this.fileModifiedTime = fileModifiedTime;
    }

    public String getPath()
    {
        return path;
    }

    public long getStart()
    {
        return start;
    }

    public long getLength()
    {
        return length;
    }

    public long getFileModifiedTime()
    {
        return fileModifiedTime;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SplitSignature signature = (SplitSignature) o;
        return start == signature.start
                && length == signature.length
                && fileModifiedTime == signature.fileModifiedTime
                && path.equals(signature.path);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(path, start, length, fileModifiedTime);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("path", path)
                .add("start", start)
                .add("length", length)
                .add("fileModifiedTime", fileModifiedTime)
                .toString();
    }
}

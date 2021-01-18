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
package io.trino.operator;

import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.Closeable;
import java.util.Optional;

@NotThreadSafe
public interface LookupSource
        extends Closeable
{
    int getChannelCount();

    long getInMemorySizeInBytes();

    long getJoinPositionCount();

    long joinPositionWithinPartition(long joinPosition);

    long getJoinPosition(int position, Page hashChannelsPage, Page allChannelsPage, long rawHash);

    long getJoinPosition(int position, Page hashChannelsPage, Page allChannelsPage);

    long getNextJoinPosition(long currentJoinPosition, int probePosition, Page allProbeChannelsPage);

    void appendTo(long position, PageBuilder pageBuilder, int outputChannelOffset);

    default void markVisited(long position)
    {
    }

    boolean isJoinPositionEligible(long currentJoinPosition, int probePosition, Page allProbeChannelsPage);

    boolean isEmpty();

    default Block getBlock(int buildChannel, Type type)
    {
        throw new UnsupportedOperationException();
    }

    default Block getBlock(int buildChannel, Type type, long[] addresses, Optional<boolean[]> valueIsNull, int arrayOffset, int positionCount)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    void close();
}

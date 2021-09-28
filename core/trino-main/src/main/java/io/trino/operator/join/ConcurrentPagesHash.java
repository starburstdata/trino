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
package io.trino.operator.join;

import com.google.common.util.concurrent.Futures;
import io.airlift.units.DataSize;
import io.trino.operator.PagesHashStrategy;
import io.trino.operator.join.ConcurrentArrayPositionLinks.ConcurrentFactoryBuilder;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import it.unimi.dsi.fastutil.HashCommon;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.trino.operator.SyntheticAddress.decodePosition;
import static io.trino.operator.SyntheticAddress.decodeSliceIndex;
import static io.trino.util.HashCollisionsEstimator.estimateNumberOfHashCollisions;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

// This implementation assumes arrays used in the hash are always a power of 2
public final class ConcurrentPagesHash
        implements PagesHash
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ConcurrentPagesHash.class).instanceSize();
    private static final DataSize CACHE_SIZE = DataSize.of(128, KILOBYTE);
    private final LongArrayList addresses;
    private final PagesHashStrategy pagesHashStrategy;

    private final int channelCount;
    private final int mask;
    private final IntAtomicArray key;
    private final long size;

    // Native array of hashes for faster collisions resolution compared
    // to accessing values in blocks. We use bytes to reduce memory foot print
    // and there is no performance gain from storing full hashes
    private final byte[] positionToHashes;
    private final long hashCollisions;
    private final double expectedHashCollisions;

    public static PagesHash create(PagesHashStrategy pagesHashStrategy, LongArrayList addresses, int pagesHashThreadCount, ConcurrentFactoryBuilder positionLinksFactoryBuilder)
    {
        long hashSize = HashCommon.bigArraySize(addresses.size(), 0.75f);
        if (hashSize > 1 << 30) {
            return new BigConcurrentPagesHash(addresses, pagesHashStrategy, positionLinksFactoryBuilder, pagesHashThreadCount);
        }
        return new ConcurrentPagesHash(addresses, pagesHashStrategy, positionLinksFactoryBuilder, pagesHashThreadCount);
    }

    public ConcurrentPagesHash(
            LongArrayList addresses,
            PagesHashStrategy pagesHashStrategy,
            ConcurrentFactoryBuilder positionLinks,
            int threadCount)
    {
        this.addresses = requireNonNull(addresses, "addresses is null");
        this.pagesHashStrategy = requireNonNull(pagesHashStrategy, "pagesHashStrategy is null");
        this.channelCount = pagesHashStrategy.getChannelCount();

        // reserve memory for the arrays
        int hashSize = HashCommon.arraySize(addresses.size(), 0.75f);

        mask = hashSize - 1;
        key = new SmallIntAtomicArray(hashSize);
        key.fill(-1);

        positionToHashes = new byte[addresses.size()];

        int threadStep = Math.max(1024, (int) Math.ceil((double) addresses.size() / threadCount));
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        try {
            List<Future<Long>> futures = IntStream.range(0, threadCount)
                    .mapToObj(threadIndex -> executor.submit(() -> putPositions(addresses, positionLinks, threadIndex * threadStep, threadStep)))
                    .collect(toImmutableList());
            hashCollisions = futures.stream().mapToLong(Futures::getUnchecked).sum();
        }
        finally {
            executor.shutdownNow();
        }

        size = sizeOf(addresses.elements()) + pagesHashStrategy.getSizeInBytes() +
                key.sizeInBytes() + sizeOf(positionToHashes);
        expectedHashCollisions = estimateNumberOfHashCollisions(addresses.size(), hashSize);
    }

    private long putPositions(LongArrayList addresses, ConcurrentFactoryBuilder positionLinks, int start, int length)
    {
        if (start >= addresses.size()) {
            // nothing to do here
            return 0;
        }
        // We will process addresses in batches, to save memory on array of hashes.
        int end = Math.min(addresses.size(), start + length);
        int positionsInStep = Math.min(length, (int) CACHE_SIZE.toBytes() / Integer.SIZE);
        long[] positionToFullHashes = new long[positionsInStep];
        long hashCollisionsLocal = 0;

        for (int step = 0; start + step * positionsInStep <= end; step++) {
            int stepBeginPosition = start + step * positionsInStep;
            int stepEndPosition = Math.min(start + (step + 1) * positionsInStep, end);
            int stepSize = stepEndPosition - stepBeginPosition;

            // First extract all hashes from blocks to native array.
            // Somehow having this as a separate loop is much faster compared
            // to extracting hashes on the fly in the loop below.
            for (int position = 0; position < stepSize; position++) {
                int realPosition = position + stepBeginPosition;
                long hash = readHashPosition(realPosition);
                positionToFullHashes[position] = hash;
                positionToHashes[realPosition] = (byte) hash;
            }

            // index pages
            for (int position = 0; position < stepSize; position++) {
                int realPosition = position + stepBeginPosition;
                if (isPositionNull(realPosition)) {
                    continue;
                }
                hashCollisionsLocal += putPosition(positionLinks, realPosition, positionToFullHashes[position]);
            }
        }
        return hashCollisionsLocal;
    }

    private long putPosition(ConcurrentFactoryBuilder positionLinks, int realPosition, long hash)
    {
        long pos = getHashPosition(hash, mask);
        long hashCollisionsLocal = 0;
        while (true) {
            // look for an empty slot or a slot containing this key
            int currentKey;
            while ((currentKey = key.get(pos)) != -1) {
                if (((byte) hash) == positionToHashes[currentKey] && positionEqualsPositionIgnoreNulls(currentKey, realPosition)) {
                    // found a slot for this key
                    positionLinks.linkAndUpdate(key, pos, currentKey, realPosition);
                    // key[pos] updated outside of this loop
                    return hashCollisionsLocal;
                }
                // increment position and mask to handler wrap around
                pos = (pos + 1) & mask;
                hashCollisionsLocal++;
            }

            if (key.compareAndSet(pos, currentKey, realPosition)) {
                // value was successfully updated
                return hashCollisionsLocal;
            }
            else {
                // value update failed, try next position
                pos = (pos + 1) & mask;
            }
        }
    }

    public final int getChannelCount()
    {
        return channelCount;
    }

    public int getPositionCount()
    {
        return addresses.size();
    }

    public long getInMemorySizeInBytes()
    {
        return INSTANCE_SIZE + size;
    }

    public long getHashCollisions()
    {
        return hashCollisions;
    }

    public double getExpectedHashCollisions()
    {
        return expectedHashCollisions;
    }

    public int getAddressIndex(int position, Page hashChannelsPage)
    {
        return getAddressIndex(position, hashChannelsPage, pagesHashStrategy.hashRow(position, hashChannelsPage));
    }

    public int getAddressIndex(int rightPosition, Page hashChannelsPage, long rawHash)
    {
        long pos = getHashPosition(rawHash, mask);

        int currentKey;
        while ((currentKey = key.getPlain(pos)) != -1) {
            if (positionEqualsCurrentRowIgnoreNulls(currentKey, (byte) rawHash, rightPosition, hashChannelsPage)) {
                return currentKey;
            }
            // increment position and mask to handler wrap around
            pos = (pos + 1) & mask;
        }
        return -1;
    }

    public void appendTo(long position, PageBuilder pageBuilder, int outputChannelOffset)
    {
        long pageAddress = addresses.getLong(toIntExact(position));
        int blockIndex = decodeSliceIndex(pageAddress);
        int blockPosition = decodePosition(pageAddress);

        pagesHashStrategy.appendTo(blockIndex, blockPosition, pageBuilder, outputChannelOffset);
    }

    private boolean isPositionNull(int position)
    {
        long pageAddress = addresses.getLong(position);
        int blockIndex = decodeSliceIndex(pageAddress);
        int blockPosition = decodePosition(pageAddress);

        return pagesHashStrategy.isPositionNull(blockIndex, blockPosition);
    }

    private long readHashPosition(int position)
    {
        long pageAddress = addresses.getLong(position);
        int blockIndex = decodeSliceIndex(pageAddress);
        int blockPosition = decodePosition(pageAddress);

        return pagesHashStrategy.hashPosition(blockIndex, blockPosition);
    }

    private boolean positionEqualsCurrentRowIgnoreNulls(int leftPosition, byte rawHash, int rightPosition, Page rightPage)
    {
        if (positionToHashes[leftPosition] != rawHash) {
            return false;
        }

        long pageAddress = addresses.getLong(leftPosition);
        int blockIndex = decodeSliceIndex(pageAddress);
        int blockPosition = decodePosition(pageAddress);

        return pagesHashStrategy.positionEqualsRowIgnoreNulls(blockIndex, blockPosition, rightPosition, rightPage);
    }

    private boolean positionEqualsPositionIgnoreNulls(int leftPosition, int rightPosition)
    {
        long leftPageAddress = addresses.getLong(leftPosition);
        int leftBlockIndex = decodeSliceIndex(leftPageAddress);
        int leftBlockPosition = decodePosition(leftPageAddress);

        long rightPageAddress = addresses.getLong(rightPosition);
        int rightBlockIndex = decodeSliceIndex(rightPageAddress);
        int rightBlockPosition = decodePosition(rightPageAddress);

        return pagesHashStrategy.positionEqualsPositionIgnoreNulls(leftBlockIndex, leftBlockPosition, rightBlockIndex, rightBlockPosition);
    }

    private static long getHashPosition(long rawHash, long mask)
    {
        // Avalanches the bits of a long integer by applying the finalisation step of MurmurHash3.
        //
        // This function implements the finalisation step of Austin Appleby's <a href="http://sites.google.com/site/murmurhash/">MurmurHash3</a>.
        // Its purpose is to avalanche the bits of the argument to within 0.25% bias. It is used, among other things, to scramble quickly (but deeply) the hash
        // values returned by {@link Object#hashCode()}.
        //

        rawHash ^= rawHash >>> 33;
        rawHash *= 0xff51afd7ed558ccdL;
        rawHash ^= rawHash >>> 33;
        rawHash *= 0xc4ceb9fe1a85ec53L;
        rawHash ^= rawHash >>> 33;

        return (rawHash & mask);
    }
}

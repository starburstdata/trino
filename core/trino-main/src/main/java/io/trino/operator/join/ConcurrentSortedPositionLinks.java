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

import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import io.trino.operator.PagesHashStrategy;
import io.trino.spi.Page;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntComparator;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.operator.SyntheticAddress.decodePosition;
import static io.trino.operator.SyntheticAddress.decodeSliceIndex;
import static java.util.Objects.requireNonNull;

/**
 * Maintains position links in sorted order by build side expression.
 * Then iteration over position links uses set of @{code searchFunctions} which needs to be compatible
 * with expression used for sorting.
 * The binary search is used to quickly skip positions which would not match filter function from join condition.
 */
public final class ConcurrentSortedPositionLinks
        implements PositionLinks
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ConcurrentSortedPositionLinks.class).instanceSize();

    public static class FactoryBuilder
            implements ConcurrentArrayPositionLinks.ConcurrentFactoryBuilder
    {
        private final ConcurrentMap<Integer, IntArrayList> positionLinks;
        private final Object[] locks;
        private final int locksMask;
        private final int size;
        private final IntComparator comparator;
        private final PagesHashStrategy pagesHashStrategy;
        private final LongArrayList addresses;

        public FactoryBuilder(int size, PagesHashStrategy pagesHashStrategy, LongArrayList addresses)
        {
            this.size = size;
            this.comparator = new PositionComparator(pagesHashStrategy, addresses);
            this.pagesHashStrategy = pagesHashStrategy;
            this.addresses = addresses;
            positionLinks = new ConcurrentHashMap<>();
            int concurrency = 1024;
            locks = new Object[concurrency];
            locksMask = concurrency - 1;
            for (int i = 0; i < locks.length; i++) {
                locks[i] = new Object();
            }
        }

        @Override
        public int link(int from, int to)
        {
            return link(from, to, 0);
        }

        @Override
        public int link(int from, int to, long pos)
        {
            // don't add _from_ row to chain if its sort channel value is null
            if (isNull(from)) {
                // _to_ row sort channel value might be null. However, in such
                // case it will be the only element in the chain, so sorted position
                // links enumeration will produce correct results.
                return to;
            }

            // don't add _to_ row to chain if its sort channel value is null
            if (isNull(to)) {
                return from;
            }

            // make sure that from value is the smaller one
            if (comparator.compare(from, to) > 0) {
                // _from_ is larger so, just add to current chain _to_
                List<Integer> links = positionLinks.computeIfAbsent(to, key -> new IntArrayList());
                links.add(from);
                return to;
            }
            else {
                // _to_ is larger so, move the chain to _from_
                IntArrayList links = positionLinks.remove(to);
                if (links == null) {
                    links = new IntArrayList();
                }
                links.add(to);
                checkState(positionLinks.putIfAbsent(from, links) == null, "sorted links is corrupted");
                return from;
            }
        }

        private boolean isNull(int position)
        {
            long pageAddress = addresses.getLong(position);
            int blockIndex = decodeSliceIndex(pageAddress);
            int blockPosition = decodePosition(pageAddress);
            return pagesHashStrategy.isSortChannelPositionNull(blockIndex, blockPosition);
        }

        @Override
        public Factory build()
        {
            ArrayPositionLinks.FactoryBuilder arrayPositionLinksFactoryBuilder = ArrayPositionLinks.builder(size);
            int[][] sortedPositionLinks = new int[size][];

            for (Map.Entry<Integer, IntArrayList> entry : positionLinks.entrySet()) {
                int key = entry.getKey();
                IntArrayList positions = entry.getValue();
                positions.sort(comparator);

                sortedPositionLinks[key] = new int[positions.size()];
                for (int i = 0; i < positions.size(); i++) {
                    sortedPositionLinks[key][i] = positions.getInt(i);
                }

                // ArrayPositionsLinks.Builder::link builds position links from
                // tail to head, so we must add them in descending order to have
                // smallest element as a head
                for (int i = positions.size() - 2; i >= 0; i--) {
                    arrayPositionLinksFactoryBuilder.link(positions.getInt(i), positions.getInt(i + 1));
                }

                // add link from starting position to position links chain
                if (!positions.isEmpty()) {
                    arrayPositionLinksFactoryBuilder.link(key, positions.getInt(0));
                }
            }

            Factory arrayPositionLinksFactory = arrayPositionLinksFactoryBuilder.build();

            return new Factory()
            {
                @Override
                public PositionLinks create(List<JoinFilterFunction> searchFunctions)
                {
                    return new ConcurrentSortedPositionLinks(
                            arrayPositionLinksFactory.create(ImmutableList.of()),
                            sortedPositionLinks,
                            searchFunctions);
                }

                @Override
                public long checksum()
                {
                    // For spill/unspill state restoration, sorted position links do not matter
                    return arrayPositionLinksFactory.checksum();
                }
            };
        }

        @Override
        public int size()
        {
            return positionLinks.size();
        }

        @Override
        public void linkAndUpdate(IntAtomicArray key, long pos, int currentKey, int realPosition)
        {
            synchronized (locks[(int) pos & locksMask]) {
                currentKey = key.get(pos);
                int positionLinkHead = link(realPosition, currentKey, pos);
                if (currentKey != positionLinkHead) {
                    Verify.verify(key.compareAndSet(pos, currentKey, positionLinkHead), "should not happen.. %s %s %s %s", pos, currentKey, realPosition, positionLinkHead);
                }
            }
        }
    }

    private final PositionLinks positionLinks;
    private final int[][] sortedPositionLinks;
    private final long sizeInBytes;
    private final JoinFilterFunction[] searchFunctions;

    private ConcurrentSortedPositionLinks(PositionLinks positionLinks, int[][] sortedPositionLinks, List<JoinFilterFunction> searchFunctions)
    {
        this.positionLinks = requireNonNull(positionLinks, "positionLinks is null");
        this.sortedPositionLinks = requireNonNull(sortedPositionLinks, "sortedPositionLinks is null");
        this.sizeInBytes = INSTANCE_SIZE + positionLinks.getSizeInBytes() + sizeOfPositionLinks(sortedPositionLinks);
        requireNonNull(searchFunctions, "searchFunctions is null");
        checkState(!searchFunctions.isEmpty(), "Using sortedPositionLinks with no search functions");
        this.searchFunctions = searchFunctions.stream().toArray(JoinFilterFunction[]::new);
    }

    private long sizeOfPositionLinks(int[][] sortedPositionLinks)
    {
        long retainedSize = sizeOf(sortedPositionLinks);
        for (int[] element : sortedPositionLinks) {
            retainedSize += sizeOf(element);
        }
        return retainedSize;
    }

    public static FactoryBuilder builder(int size, PagesHashStrategy pagesHashStrategy, LongArrayList addresses)
    {
        return new FactoryBuilder(size, pagesHashStrategy, addresses);
    }

    @Override
    public int next(int position, int probePosition, Page allProbeChannelsPage)
    {
        int nextPosition = positionLinks.next(position, probePosition, allProbeChannelsPage);
        if (nextPosition < 0) {
            return -1;
        }
        if (!applyAllSearchFunctions(nextPosition, probePosition, allProbeChannelsPage)) {
            // break a position links chain if next position should be filtered out
            return -1;
        }
        return nextPosition;
    }

    @Override
    public int start(int startingPosition, int probePosition, Page allProbeChannelsPage)
    {
        if (applyAllSearchFunctions(startingPosition, probePosition, allProbeChannelsPage)) {
            return startingPosition;
        }
        int[] links = sortedPositionLinks[startingPosition];
        if (links == null) {
            return -1;
        }
        int currentStartOffset = 0;
        for (JoinFilterFunction searchFunction : searchFunctions) {
            currentStartOffset = findStartPositionForFunction(searchFunction, links, currentStartOffset, probePosition, allProbeChannelsPage);
            // return as soon as a mismatch is found, since we are handling only AND predicates (conjuncts)
            if (currentStartOffset == -1) {
                return -1;
            }
        }
        return links[currentStartOffset];
    }

    private boolean applyAllSearchFunctions(int buildPosition, int probePosition, Page allProbeChannelsPage)
    {
        for (JoinFilterFunction searchFunction : searchFunctions) {
            if (!applySearchFunction(searchFunction, buildPosition, probePosition, allProbeChannelsPage)) {
                return false;
            }
        }
        return true;
    }

    private int findStartPositionForFunction(JoinFilterFunction searchFunction, int[] links, int startOffset, int probePosition, Page allProbeChannelsPage)
    {
        if (applySearchFunction(searchFunction, links, startOffset, probePosition, allProbeChannelsPage)) {
            // MAJOR HACK: if searchFunction is of shape `f(probe) > build_symbol` it is not fit for binary search below,
            // but it does not imply extra constraints on start position; so we just ignore it.
            // It does not break logic for `f(probe) < build_symbol` as the binary search below would return same value.

            // todo: Explicitly handle less-than and greater-than functions separately.
            return startOffset;
        }

        // do a binary search for the first position for which filter function applies
        int offset = lowerBound(searchFunction, links, startOffset, links.length - 1, probePosition, allProbeChannelsPage);
        if (!applySearchFunction(searchFunction, links, offset, probePosition, allProbeChannelsPage)) {
            return -1;
        }
        return offset;
    }

    /**
     * Find the first element in position links that is NOT smaller than probePosition
     */
    private int lowerBound(JoinFilterFunction searchFunction, int[] links, int first, int last, int probePosition, Page allProbeChannelsPage)
    {
        int middle;
        int step;
        int count = last - first;
        while (count > 0) {
            step = count / 2;
            middle = first + step;
            if (!applySearchFunction(searchFunction, links, middle, probePosition, allProbeChannelsPage)) {
                first = ++middle;
                count -= step + 1;
            }
            else {
                count = step;
            }
        }
        return first;
    }

    @Override
    public long getSizeInBytes()
    {
        return sizeInBytes;
    }

    private boolean applySearchFunction(JoinFilterFunction searchFunction, int[] links, int linkOffset, int probePosition, Page allProbeChannelsPage)
    {
        return applySearchFunction(searchFunction, links[linkOffset], probePosition, allProbeChannelsPage);
    }

    private boolean applySearchFunction(JoinFilterFunction searchFunction, long buildPosition, int probePosition, Page allProbeChannelsPage)
    {
        return searchFunction.filter((int) buildPosition, probePosition, allProbeChannelsPage);
    }

    private static class PositionComparator
            implements IntComparator
    {
        private final PagesHashStrategy pagesHashStrategy;
        private final LongArrayList addresses;

        PositionComparator(PagesHashStrategy pagesHashStrategy, LongArrayList addresses)
        {
            this.pagesHashStrategy = pagesHashStrategy;
            this.addresses = addresses;
        }

        @Override
        public int compare(int leftPosition, int rightPosition)
        {
            long leftPageAddress = addresses.getLong(leftPosition);
            int leftBlockIndex = decodeSliceIndex(leftPageAddress);
            int leftBlockPosition = decodePosition(leftPageAddress);

            long rightPageAddress = addresses.getLong(rightPosition);
            int rightBlockIndex = decodeSliceIndex(rightPageAddress);
            int rightBlockPosition = decodePosition(rightPageAddress);

            return pagesHashStrategy.compareSortChannelPositions(leftBlockIndex, leftBlockPosition, rightBlockIndex, rightBlockPosition);
        }

        @Override
        public int compare(Integer leftPosition, Integer rightPosition)
        {
            return compare(leftPosition.intValue(), rightPosition.intValue());
        }
    }
}

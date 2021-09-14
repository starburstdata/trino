package io.trino.operator.join;

import io.trino.operator.PagesHashStrategy;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import it.unimi.dsi.fastutil.HashCommon;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.openjdk.jol.info.ClassLayout;

import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.operator.SyntheticAddress.decodePosition;
import static io.trino.operator.SyntheticAddress.decodeSliceIndex;
import static io.trino.util.HashCollisionsEstimator.estimateNumberOfHashCollisions;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class BigIntPagesHash
        implements PagesHash
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(GenericPagesHash.class).instanceSize();

    private final LongArrayList addresses;
    private final PagesHashStrategy pagesHashStrategy;

    private final int channelCount;
    private final long size;
    private final long hashCollisions;
    private final double expectedHashCollisions;
    private final Long2IntOpenHashMap hashChannelValueToPosition;

    public BigIntPagesHash(
            LongArrayList addresses,
            PagesHashStrategy pagesHashStrategy,
            PositionLinks.FactoryBuilder positionLinks,
            HashChannels hashChannels)
    {
        this.addresses = requireNonNull(addresses, "addresses is null");
        this.pagesHashStrategy = requireNonNull(pagesHashStrategy, "pagesHashStrategy is null");
        this.channelCount = pagesHashStrategy.getChannelCount();

        // reserve memory for the arrays
        float loadFactor = 0.5f;
        int hashSize = HashCommon.arraySize(addresses.size(), loadFactor);
        Long2IntOpenHashMap hashChannelValueToPosition = new Long2IntOpenHashMap(addresses.size(), loadFactor);
        hashChannelValueToPosition.defaultReturnValue(-1);

        for (int position = 0; position < addresses.size(); position++) {
            long pageAddress = addresses.getLong(position);
            int blockIndex = decodeSliceIndex(pageAddress);
            int blockPosition = decodePosition(pageAddress);

            Block hashChannel = hashChannels.getHashChannel(blockIndex, 0);
            if (hashChannel.isNull(blockPosition)) {
                break;
            }
            long hashChannelValue = hashChannel.getLong(blockPosition, 0);
            int oldValue = hashChannelValueToPosition.put(hashChannelValue, position);
            if (oldValue != -1) {
                int positionToKeep = positionLinks.link(position, oldValue);
                // TODO lysy: this can be optimized, we only have to find the p[osition first and then set the final value
                // This may be hard to do with Long2IntOpenHashMap
                hashChannelValueToPosition.put(hashChannelValue, positionToKeep);
            }
        }

        size = sizeOf(addresses.elements()) + pagesHashStrategy.getSizeInBytes();
        hashCollisions = 0; // unknown in Long2IntOpenHashMap
        expectedHashCollisions = estimateNumberOfHashCollisions(addresses.size(), hashSize);
        this.hashChannelValueToPosition = hashChannelValueToPosition;
    }

    @Override
    public final int getChannelCount()
    {
        return channelCount;
    }

    @Override
    public int getPositionCount()
    {
        return addresses.size();
    }

    @Override
    public long getInMemorySizeInBytes()
    {
        return INSTANCE_SIZE + size;
    }

    @Override
    public long getHashCollisions()
    {
        return hashCollisions;
    }

    @Override
    public double getExpectedHashCollisions()
    {
        return expectedHashCollisions;
    }

    @Override
    public int getAddressIndex(int rightPosition, Page hashChannelsPage, long rawHash)
    {
        return getAddressIndex(rightPosition, hashChannelsPage);
    }

    @Override
    public int getAddressIndex(int position, Page hashChannelsPage)
    {
        long hashChannelValue = hashChannelsPage.getBlock(0).getLong(position, 0);

        return hashChannelValueToPosition.get(hashChannelValue);
    }

    @Override
    public void appendTo(long position, PageBuilder pageBuilder, int outputChannelOffset)
    {
        long pageAddress = addresses.getLong(toIntExact(position));
        int blockIndex = decodeSliceIndex(pageAddress);
        int blockPosition = decodePosition(pageAddress);

        pagesHashStrategy.appendTo(blockIndex, blockPosition, pageBuilder, outputChannelOffset);
    }
}

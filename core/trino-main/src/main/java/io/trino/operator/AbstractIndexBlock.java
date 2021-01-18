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

import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public abstract class AbstractIndexBlock
        implements Block
{
    private final Type type;
    private final long[] addresses;
    @Nullable
    private final boolean[] valueIsNull;
    private final int arrayOffset;
    private final int positionCount;

    private long logicalSizeInBytes = -1;

    protected AbstractIndexBlock(Type type, long[] addresses, @Nullable boolean[] valueIsNull, int arrayOffset, int positionCount)
    {
        this.type = requireNonNull(type, "type is null");
        this.addresses = requireNonNull(addresses, "addresses is null");
        this.valueIsNull = valueIsNull;
        this.arrayOffset = arrayOffset;
        this.positionCount = positionCount;

        checkArgument(arrayOffset >= 0 && arrayOffset <= addresses.length, "arrayOffset is not valid");
        checkArgument(positionCount >= 0 && positionCount <= addresses.length - arrayOffset, "positionCount is not valid");
    }

    protected abstract Block createIndexBlock(long[] addresses, @Nullable boolean[] valueIsNull, int arrayOffset, int positionCount);

    protected abstract Block getBlock(long address);

    protected abstract int getBlockPosition(long address);

    @Override
    public int getSliceLength(int position)
    {
        long address = getAddress(position);
        return getBlock(address).getSliceLength(getBlockPosition(address));
    }

    @Override
    public byte getByte(int position, int offset)
    {
        long address = getAddress(position);
        return getBlock(address).getByte(getBlockPosition(address), offset);
    }

    @Override
    public short getShort(int position, int offset)
    {
        long address = getAddress(position);
        return getBlock(address).getShort(getBlockPosition(address), offset);
    }

    @Override
    public int getInt(int position, int offset)
    {
        long address = getAddress(position);
        return getBlock(address).getInt(getBlockPosition(address), offset);
    }

    @Override
    public long getLong(int position, int offset)
    {
        long address = getAddress(position);
        return getBlock(address).getLong(getBlockPosition(address), offset);
    }

    @Override
    public Slice getSlice(int position, int offset, int length)
    {
        long address = getAddress(position);
        return getBlock(address).getSlice(getBlockPosition(address), offset, length);
    }

    @Override
    public <T> T getObject(int position, Class<T> clazz)
    {
        long address = getAddress(position);
        return getBlock(address).getObject(getBlockPosition(address), clazz);
    }

    @Override
    public boolean bytesEqual(int position, int offset, Slice otherSlice, int otherOffset, int length)
    {
        long address = getAddress(position);
        return getBlock(address).bytesEqual(getBlockPosition(address), offset, otherSlice, otherOffset, length);
    }

    @Override
    public int bytesCompare(int position, int offset, int length, Slice otherSlice, int otherOffset, int otherLength)
    {
        long address = getAddress(position);
        return getBlock(address).bytesCompare(getBlockPosition(address), offset, length, otherSlice, otherOffset, otherLength);
    }

    @Override
    public void writeBytesTo(int position, int offset, int length, BlockBuilder blockBuilder)
    {
        long address = getAddress(position);
        getBlock(address).writeBytesTo(getBlockPosition(address), offset, length, blockBuilder);
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder)
    {
        long address = getAddress(position);
        getBlock(address).writePositionTo(getBlockPosition(address), blockBuilder);
    }

    @Override
    public boolean equals(int position, int offset, Block otherBlock, int otherPosition, int otherOffset, int length)
    {
        long address = getAddress(position);
        return getBlock(address).equals(getBlockPosition(address), offset, otherBlock, otherPosition, otherOffset, length);
    }

    @Override
    public long hash(int position, int offset, int length)
    {
        long address = getAddress(position);
        return getBlock(address).hash(getBlockPosition(address), offset, length);
    }

    @Override
    public int compareTo(int leftPosition, int leftOffset, int leftLength, Block rightBlock, int rightPosition, int rightOffset, int rightLength)
    {
        long address = getAddress(leftPosition);
        return getBlock(address).compareTo(getBlockPosition(address), leftOffset, leftLength, rightBlock, rightPosition, rightOffset, rightLength);
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        long address = getAddress(position);

        if (isNull(position, address)) {
            return createIndexBlock(new long[] {address}, new boolean[] {true}, 0, 1);
        }

        return getBlock(address).getSingleValueBlock(getBlockPosition(address));
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public long getSizeInBytes()
    {
        // backing blocks are not owned by index block
        return 0;
    }

    @Override
    public long getLogicalSizeInBytes()
    {
        if (logicalSizeInBytes < 0) {
            long newLogicalSizeInBytes = 0;
            for (int i = arrayOffset; i < arrayOffset + positionCount; ++i) {
                long address = addresses[i];
                newLogicalSizeInBytes += getBlock(address).getRegionSizeInBytes(getBlockPosition(address), 1);
            }
            logicalSizeInBytes = newLogicalSizeInBytes;
        }

        return logicalSizeInBytes;
    }

    @Override
    public long getRegionSizeInBytes(int positionOffset, int length)
    {
        boolean[] used = new boolean[positionCount];
        for (int position = positionOffset; position < positionOffset + length; ++position) {
            used[position] = true;
        }
        return getPositionsSizeInBytes(used);
    }

    @Override
    public long getPositionsSizeInBytes(boolean[] used)
    {
        Map<Block, boolean[]> blockUsed = new HashMap<>();
        for (int position = 0; position < used.length; ++position) {
            long address = getAddress(position);
            Block block = getBlock(address);
            int blockPosition = getBlockPosition(address);
            blockUsed.computeIfAbsent(block, ignored -> new boolean[block.getPositionCount()])[blockPosition] = true;
        }

        long sizeInBytes = 0;
        for (Map.Entry<Block, boolean[]> entry : blockUsed.entrySet()) {
            sizeInBytes += entry.getKey().getPositionsSizeInBytes(entry.getValue());
        }

        return sizeInBytes;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        // backing blocks are not owned by index block
        return 0;
    }

    @Override
    public long getEstimatedDataSizeForStats(int position)
    {
        long address = getAddress(position);

        if (isNull(position, address)) {
            return 0;
        }

        return getBlock(address).getEstimatedDataSizeForStats(getBlockPosition(address));
    }

    @Override
    public void retainedBytesForEachPart(BiConsumer<Object, Long> consumer)
    {
        // backing blocks are not owned by index block
    }

    @Override
    public String getEncodingName()
    {
        return IndexBlockEncoding.NAME;
    }

    @Override
    public Block getPositions(int[] positions, int offset, int length)
    {
        long[] newAddresses = new long[length];
        for (int i = 0; i < length; ++i) {
            newAddresses[i] = getAddress(positions[offset + i]);
        }

        boolean[] newValueIsNull = null;
        if (valueIsNull != null) {
            newValueIsNull = new boolean[length];
            for (int i = 0; i < length; ++i) {
                newValueIsNull[i] = isNull(positions[offset + i]);
            }
        }

        return createIndexBlock(newAddresses, newValueIsNull, 0, length);
    }

    @Override
    public Block copyPositions(int[] positions, int offset, int length)
    {
        BlockBuilder builder = type.createBlockBuilder(null, length);
        for (int i = 0; i < length; ++i) {
            int position = positions[offset + i];
            long address = getAddress(position);
            if (isNull(position, address)) {
                builder.appendNull();
            }
            else {
                type.appendTo(getBlock(address), getBlockPosition(address), builder);
            }
        }
        return builder.build();
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        return createIndexBlock(addresses, valueIsNull, arrayOffset + positionOffset, length);
    }

    @Override
    public Block copyRegion(int positionOffset, int length)
    {
        BlockBuilder builder = type.createBlockBuilder(null, length);
        for (int position = positionOffset; position < positionOffset + length; ++position) {
            long address = getAddress(position);
            if (isNull(position, address)) {
                builder.appendNull();
            }
            else {
                type.appendTo(getBlock(address), getBlockPosition(address), builder);
            }
        }
        return builder.build();
    }

    @Override
    public boolean mayHaveNull()
    {
        return valueIsNull != null;
    }

    @Override
    public boolean isNull(int position)
    {
        checkArgument(position >= 0 && position < positionCount, "position is not valid");
        if (valueIsNull != null && valueIsNull[arrayOffset + position]) {
            return true;
        }

        long address = getAddress(position);
        return getBlock(address).isNull(getBlockPosition(address));
    }

    private boolean isNull(int position, long address)
    {
        checkArgument(position >= 0 && position < positionCount, "position is not valid");
        if (valueIsNull != null && valueIsNull[arrayOffset + position]) {
            return true;
        }

        return getBlock(address).isNull(getBlockPosition(address));
    }

    @Override
    public boolean isLoaded()
    {
        // indexed blocks are always loaded
        return true;
    }

    @Override
    public Block getLoadedBlock()
    {
        return this;
    }

    @Override
    public Block getBlockForStorage()
    {
        return copyRegion(0, positionCount);
    }

    Type getType()
    {
        return type;
    }

    private long getAddress(int position)
    {
        checkArgument(position >= 0 && position < positionCount, "position is not valid");
        return addresses[arrayOffset + position];
    }
}

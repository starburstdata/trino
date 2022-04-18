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
package io.trino.operator.output;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilderStatus;
import io.trino.spi.block.BlockUtil;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.openjdk.jmh.annotations.CompilerControl;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Optional;

import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.spi.block.BlockUtil.calculateBlockResetSize;
import static java.lang.Math.max;

public class BytePositionsAppender
        implements BlockTypeAwarePositionsAppender
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(BytePositionsAppender.class).instanceSize();
    private static final Block NULL_VALUE_BLOCK = new ByteArrayBlock(1, Optional.of(new boolean[] {true}), new byte[1]);

    @Nullable
    private final BlockBuilderStatus blockBuilderStatus;
    private boolean initialized;
    private final int initialEntryCount;

    private int positionCount;
    private boolean hasNullValue;
    private boolean hasNonNullValue;

    // it is assumed that these arrays are the same length
    private boolean[] valueIsNull = new boolean[0];
    private byte[] values = new byte[0];

    private long retainedSizeInBytes;

    public BytePositionsAppender(@Nullable BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        this.blockBuilderStatus = blockBuilderStatus;
        this.initialEntryCount = max(expectedEntries, 1);

        updateDataSize();
    }

    @Override
    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    public void append(IntArrayList positions, Block block)
    {
        int[] positionArray = positions.elements();
        int newPositionCount = positions.size();
        ensureCapacity(positionCount + newPositionCount);

        if (block.mayHaveNull()) {
            appendNullable(block, positionArray, newPositionCount);
            this.positionCount += newPositionCount;
        }
        else {
            appendNotNull(block, positionArray, newPositionCount);
            positionCount += newPositionCount;
        }

        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(ByteArrayBlock.SIZE_IN_BYTES_PER_POSITION * newPositionCount);
        }
    }

    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    private void appendNotNull(Block block, int[] positionArray, int newPositionCount)
    {
        for (int i = 0; i < newPositionCount; i++) {
            int position = positionArray[i];
            values[positionCount + i] = block.getByte(position, 0);
        }
        this.hasNonNullValue = true;
    }

    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    private void appendNullableBranchless(Block block, int[] positionArray, int newPositionCount)
    {
        boolean hasNullValue = false;
        boolean hasNonNullValue = false;
        for (int i = 0; i < newPositionCount; i++) {
            int position = positionArray[i];
            boolean isNull = block.isNull(position);
            int positionIndex = positionCount + i;

            valueIsNull[positionIndex] = isNull;
            hasNullValue = isNull;

            values[positionIndex] = isNull ? values[positionIndex] : block.getByte(position, 0);
            hasNonNullValue = !isNull;
        }
        this.hasNullValue |= hasNullValue;
        this.hasNonNullValue |= hasNonNullValue;
    }

    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    private void appendNullable(Block block, int[] positionArray, int newPositionCount)
    {
//        boolean hasNullValue = false;
//        boolean hasNonNullValue = false;
        for (int i = 0; i < newPositionCount; i++) {
            int position = positionArray[i];
            boolean isNull = block.isNull(position);
            int positionIndex = positionCount + i;
            if (isNull) {
                valueIsNull[positionIndex] = true;
                hasNullValue = true;
            }
            else {
                values[positionIndex] = block.getByte(position, 0);
                hasNonNullValue = true;
            }
        }
//        this.hasNullValue |= hasNullValue;
//        this.hasNonNullValue |= hasNonNullValue;
    }

    //    @Override
    public void appendDictionary2(IntArrayList positions, DictionaryBlock block)
    {
        int[] positionArray = positions.elements();
        int newPositionCount = positions.size();
        ensureCapacity(positionCount + newPositionCount);
        if (block.mayHaveNull()) {
            for (int i = 0; i < newPositionCount; i++) {
                int position = positionArray[i];
                if (block.isNull(position)) {
                    valueIsNull[positionCount] = true;
                    hasNullValue = true;
                }
                else {
                    values[positionCount] = block.getByte(position, 0);
                    hasNonNullValue = true;
                }
                positionCount++;
            }
        }
        else {
            for (int i = 0; i < newPositionCount; i++) {
                int position = positionArray[i];
                values[positionCount] = block.getByte(position, 0);
                positionCount++;
            }
            hasNonNullValue = true;
        }

        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(ByteArrayBlock.SIZE_IN_BYTES_PER_POSITION * newPositionCount);
        }
    }

    //    @Override
    public void appendDictionary(IntArrayList positions, DictionaryBlock block)
    {
        append(mapPositions(positions, block), block.getDictionary());
    }

    private IntArrayList mapPositions(IntArrayList positions, DictionaryBlock block)
    {
        int[] positionArray = new int[positions.size()];
        for (int i = 0; i < positions.size(); i++) {
            positionArray[i] = block.getId(positions.getInt(i));
        }
        return IntArrayList.wrap(positionArray);
    }

    @Override
    public void appendRle(RunLengthEncodedBlock block)
    {
        int rlePositionCount = block.getPositionCount();
        int sourcePosition = 0;
        ensureCapacity(positionCount + rlePositionCount);
        if (block.isNull(sourcePosition)) {
            Arrays.fill(valueIsNull, positionCount, positionCount + rlePositionCount, true);
            hasNullValue = true;
        }
        else {
            byte value = block.getByte(sourcePosition, 0);
            Arrays.fill(values, positionCount, positionCount + rlePositionCount, value);
            hasNonNullValue = true;
        }
        positionCount += rlePositionCount;

        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(ByteArrayBlock.SIZE_IN_BYTES_PER_POSITION * rlePositionCount);
        }
    }

    @Override
    public Block build()
    {
        if (!hasNonNullValue) {
            return new RunLengthEncodedBlock(NULL_VALUE_BLOCK, positionCount);
        }
        return new ByteArrayBlock(positionCount, hasNullValue ? Optional.of(valueIsNull) : Optional.empty(), values);
    }

    @Override
    public BlockTypeAwarePositionsAppender newStateLike(@Nullable BlockBuilderStatus blockBuilderStatus)
    {
        return new BytePositionsAppender(blockBuilderStatus, calculateBlockResetSize(positionCount));
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
    }

    private void ensureCapacity(int capacity)
    {
        if (values.length >= capacity) {
            return;
        }

        int newSize;
        if (initialized) {
            newSize = BlockUtil.calculateNewArraySize(values.length);
        }
        else {
            newSize = initialEntryCount;
            initialized = true;
        }
        newSize = Math.max(newSize, capacity);

        valueIsNull = Arrays.copyOf(valueIsNull, newSize);
        values = Arrays.copyOf(values, newSize);
        updateDataSize();
    }

    private void updateDataSize()
    {
        retainedSizeInBytes = INSTANCE_SIZE + sizeOf(valueIsNull) + sizeOf(values);
        if (blockBuilderStatus != null) {
            retainedSizeInBytes += BlockBuilderStatus.INSTANCE_SIZE;
        }
    }
}

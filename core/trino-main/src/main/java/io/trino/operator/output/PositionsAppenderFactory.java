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

import io.trino.operator.output.BlockBuilderPositionsAppender.TypeBlockBuilderPositionsAppender;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.BlockBuilderStatus;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.Int128ArrayBlock;
import io.trino.spi.block.Int96ArrayBlock;
import io.trino.spi.type.FixedWidthType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VariableWidthType;
import io.trino.type.BlockTypeOperators;
import io.trino.type.BlockTypeOperators.BlockPositionEqual;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static java.util.Objects.requireNonNull;

public class PositionsAppenderFactory
{
    private final BlockTypeOperators blockTypeOperators;

    public PositionsAppenderFactory(BlockTypeOperators blockTypeOperators)
    {
        this.blockTypeOperators = requireNonNull(blockTypeOperators, "blockTypeOperators is null");
    }

    public PositionsAppender create(Type type, BlockBuilderStatus blockBuilderStatus, int expectedPositions)
    {
        return new BlockTypeDispatchingPositionsAppender(
                new AdaptivePositionsAppender(
                        createDedicatedAppenderFor(type, getEqualOperator(type), blockBuilderStatus, expectedPositions), blockBuilderStatus, expectedPositions));
    }

    private BlockPositionEqual getEqualOperator(Type type)
    {
        if (type.isComparable()) {
            return blockTypeOperators.getEqualOperator(type);
        }
        else {
            // if type is not comparable, we are not going to be able to support different RLE values
            return (left, leftPosition, right, rightPosition) -> false;
        }
    }

    private PositionsAppender createDedicatedAppenderFor(Type type, BlockPositionEqual equalOperator, BlockBuilderStatus blockBuilderStatus, int expectedPositions)
    {
        if (type instanceof FixedWidthType) {
            switch (((FixedWidthType) type).getFixedSize()) {
                case Byte.BYTES:
                    return new BytePositionsAppender(type, equalOperator, blockBuilderStatus, expectedPositions);
                case Short.BYTES:
                    return new SmallintPositionsAppender(type, equalOperator, blockBuilderStatus, expectedPositions);
                case Integer.BYTES:
                    return new IntPositionsAppender(type, equalOperator, blockBuilderStatus, expectedPositions);
                case Long.BYTES:
                    return new LongPositionsAppender(type, equalOperator, blockBuilderStatus, expectedPositions);
                case Int96ArrayBlock.INT96_BYTES:
                    return new Int96PositionsAppender(type, equalOperator, blockBuilderStatus, expectedPositions);
                case Int128ArrayBlock.INT128_BYTES:
                    return new Int128PositionsAppender(type, equalOperator, blockBuilderStatus, expectedPositions);
                default:
                    // size not supported directly, fallback to the generic appender
            }
        }
        else if (type instanceof VariableWidthType) {
            return new SlicePositionsAppender(type, equalOperator, blockBuilderStatus, expectedPositions);
        }

        return new TypeBlockBuilderPositionsAppender(type, equalOperator, blockBuilderStatus, expectedPositions);
    }

    public static class LongPositionsAppender
            extends BlockBuilderPositionsAppender
    {
        public LongPositionsAppender(Type type, BlockPositionEqual equalOperator, BlockBuilderStatus blockBuilderStatus, int expectedPositions)
        {
            super(type, equalOperator, blockBuilderStatus, expectedPositions);
        }

        @Override
        public void appendTo(IntArrayList positions, Block block, BlockBuilder blockBuilder)
        {
            int[] positionArray = positions.elements();
            if (block.mayHaveNull()) {
                for (int i = 0; i < positions.size(); i++) {
                    int position = positionArray[i];
                    if (block.isNull(position)) {
                        blockBuilder.appendNull();
                    }
                    else {
                        blockBuilder.writeLong(block.getLong(position, 0)).closeEntry();
                    }
                }
            }
            else {
                for (int i = 0; i < positions.size(); i++) {
                    blockBuilder.writeLong(block.getLong(positionArray[i], 0)).closeEntry();
                }
            }
        }

        @Override
        protected void appendDictionary(IntArrayList positions, DictionaryBlock block, BlockBuilder blockBuilder)
        {
            int[] positionArray = positions.elements();
            if (block.mayHaveNull()) {
                for (int i = 0; i < positions.size(); i++) {
                    int position = positionArray[i];
                    if (block.isNull(position)) {
                        blockBuilder.appendNull();
                    }
                    else {
                        blockBuilder.writeLong(block.getLong(position, 0)).closeEntry();
                    }
                }
            }
            else {
                for (int i = 0; i < positions.size(); i++) {
                    blockBuilder.writeLong(block.getLong(positionArray[i], 0)).closeEntry();
                }
            }
        }

        @Override
        protected void appendRle(int positionCount, Block block, int sourcePosition, BlockBuilder blockBuilder)
        {
            if (block.isNull(sourcePosition)) {
                for (int i = 0; i < positionCount; i++) {
                    blockBuilder.appendNull();
                }
            }
            else {
                long value = block.getLong(sourcePosition, 0);
                for (int i = 0; i < positionCount; i++) {
                    blockBuilder.writeLong(value).closeEntry();
                }
            }
        }

        @Override
        public PositionsAppender newState(BlockBuilderStatus blockBuilderStatus, int expectedPositions)
        {
            return new LongPositionsAppender(type, equalOperator, blockBuilderStatus, expectedPositions);
        }
    }

    public static class IntPositionsAppender
            extends BlockBuilderPositionsAppender
    {
        public IntPositionsAppender(Type type, BlockPositionEqual equalOperator, BlockBuilderStatus blockBuilderStatus, int expectedPositions)
        {
            super(type, equalOperator, blockBuilderStatus, expectedPositions);
        }

        @Override
        public void appendTo(IntArrayList positions, Block block, BlockBuilder blockBuilder)
        {
            int[] positionArray = positions.elements();
            if (block.mayHaveNull()) {
                for (int i = 0; i < positions.size(); i++) {
                    int position = positionArray[i];
                    if (block.isNull(position)) {
                        blockBuilder.appendNull();
                    }
                    else {
                        blockBuilder.writeInt(block.getInt(position, 0)).closeEntry();
                    }
                }
            }
            else {
                for (int i = 0; i < positions.size(); i++) {
                    blockBuilder.writeInt(block.getInt(positionArray[i], 0)).closeEntry();
                }
            }
        }

        @Override
        protected void appendDictionary(IntArrayList positions, DictionaryBlock block, BlockBuilder blockBuilder)
        {
            int[] positionArray = positions.elements();
            if (block.mayHaveNull()) {
                for (int i = 0; i < positions.size(); i++) {
                    int position = positionArray[i];
                    if (block.isNull(position)) {
                        blockBuilder.appendNull();
                    }
                    else {
                        blockBuilder.writeInt(block.getInt(position, 0)).closeEntry();
                    }
                }
            }
            else {
                for (int i = 0; i < positions.size(); i++) {
                    blockBuilder.writeInt(block.getInt(positionArray[i], 0)).closeEntry();
                }
            }
        }

        @Override
        protected void appendRle(int positionCount, Block block, int sourcePosition, BlockBuilder blockBuilder)
        {
            if (block.isNull(sourcePosition)) {
                for (int i = 0; i < positionCount; i++) {
                    blockBuilder.appendNull();
                }
            }
            else {
                int value = block.getInt(sourcePosition, 0);
                for (int i = 0; i < positionCount; i++) {
                    blockBuilder.writeInt(value).closeEntry();
                }
            }
        }

        @Override
        public PositionsAppender newState(BlockBuilderStatus blockBuilderStatus, int expectedPositions)
        {
            return new IntPositionsAppender(type, equalOperator, blockBuilderStatus, expectedPositions);
        }
    }

    public static class BytePositionsAppender
            extends BlockBuilderPositionsAppender
    {
        public BytePositionsAppender(Type type, BlockPositionEqual equalOperator, BlockBuilderStatus blockBuilderStatus, int expectedPositions)
        {
            super(type, equalOperator, blockBuilderStatus, expectedPositions);
        }

        @Override
        public void appendTo(IntArrayList positions, Block block, BlockBuilder blockBuilder)
        {
            int[] positionArray = positions.elements();
            if (block.mayHaveNull()) {
                for (int i = 0; i < positions.size(); i++) {
                    int position = positionArray[i];
                    if (block.isNull(position)) {
                        blockBuilder.appendNull();
                    }
                    else {
                        blockBuilder.writeByte(block.getByte(position, 0)).closeEntry();
                    }
                }
            }
            else {
                for (int i = 0; i < positions.size(); i++) {
                    blockBuilder.writeByte(block.getByte(positionArray[i], 0)).closeEntry();
                }
            }
        }

        @Override
        protected void appendDictionary(IntArrayList positions, DictionaryBlock block, BlockBuilder blockBuilder)
        {
            int[] positionArray = positions.elements();
            if (block.mayHaveNull()) {
                for (int i = 0; i < positions.size(); i++) {
                    int position = positionArray[i];
                    if (block.isNull(position)) {
                        blockBuilder.appendNull();
                    }
                    else {
                        blockBuilder.writeByte(block.getByte(position, 0)).closeEntry();
                    }
                }
            }
            else {
                for (int i = 0; i < positions.size(); i++) {
                    blockBuilder.writeByte(block.getByte(positionArray[i], 0)).closeEntry();
                }
            }
        }

        @Override
        protected void appendRle(int positionCount, Block block, int sourcePosition, BlockBuilder blockBuilder)
        {
            if (block.isNull(sourcePosition)) {
                for (int i = 0; i < positionCount; i++) {
                    blockBuilder.appendNull();
                }
            }
            else {
                byte value = block.getByte(sourcePosition, 0);
                for (int i = 0; i < positionCount; i++) {
                    blockBuilder.writeByte(value).closeEntry();
                }
            }
        }

        @Override
        public PositionsAppender newState(BlockBuilderStatus blockBuilderStatus, int expectedPositions)
        {
            return new BytePositionsAppender(type, equalOperator, blockBuilderStatus, expectedPositions);
        }
    }

    public static class SlicePositionsAppender
            extends BlockBuilderPositionsAppender
    {
        public SlicePositionsAppender(Type type, BlockPositionEqual equalOperator, BlockBuilderStatus blockBuilderStatus, int expectedPositions)
        {
            super(type, equalOperator, blockBuilderStatus, expectedPositions);
        }

        @Override
        public void appendTo(IntArrayList positions, Block block, BlockBuilder blockBuilder)
        {
            int[] positionArray = positions.elements();
            if (block.mayHaveNull()) {
                for (int i = 0; i < positions.size(); i++) {
                    int position = positionArray[i];
                    if (block.isNull(position)) {
                        blockBuilder.appendNull();
                    }
                    else {
                        block.writeBytesTo(position, 0, block.getSliceLength(position), blockBuilder);
                        blockBuilder.closeEntry();
                    }
                }
            }
            else {
                for (int i = 0; i < positions.size(); i++) {
                    int position = positionArray[i];
                    block.writeBytesTo(position, 0, block.getSliceLength(position), blockBuilder);
                    blockBuilder.closeEntry();
                }
            }
        }

        @Override
        protected void appendDictionary(IntArrayList positions, DictionaryBlock block, BlockBuilder blockBuilder)
        {
            int[] positionArray = positions.elements();
            if (block.mayHaveNull()) {
                for (int i = 0; i < positions.size(); i++) {
                    int position = positionArray[i];
                    if (block.isNull(position)) {
                        blockBuilder.appendNull();
                    }
                    else {
                        block.writeBytesTo(position, 0, block.getSliceLength(position), blockBuilder);
                        blockBuilder.closeEntry();
                    }
                }
            }
            else {
                for (int i = 0; i < positions.size(); i++) {
                    int position = positionArray[i];
                    block.writeBytesTo(position, 0, block.getSliceLength(position), blockBuilder);
                    blockBuilder.closeEntry();
                }
            }
        }

        @Override
        protected void appendRle(int positionCount, Block block, int sourcePosition, BlockBuilder blockBuilder)
        {
            if (block.isNull(sourcePosition)) {
                for (int i = 0; i < positionCount; i++) {
                    blockBuilder.appendNull();
                }
            }
            else {
                int sliceLength = block.getSliceLength(sourcePosition);
                for (int i = 0; i < positionCount; i++) {
                    block.writeBytesTo(sourcePosition, 0, sliceLength, blockBuilder);
                    blockBuilder.closeEntry();
                }
            }
        }

        @Override
        public PositionsAppender newState(BlockBuilderStatus blockBuilderStatus, int expectedPositions)
        {
            return new SlicePositionsAppender(type, equalOperator, blockBuilderStatus, expectedPositions);
        }
    }

    public static class SmallintPositionsAppender
            extends BlockBuilderPositionsAppender
    {
        public SmallintPositionsAppender(Type type, BlockPositionEqual equalOperator, BlockBuilderStatus blockBuilderStatus, int expectedPositions)
        {
            super(type, equalOperator, blockBuilderStatus, expectedPositions);
        }

        @Override
        public void appendTo(IntArrayList positions, Block block, BlockBuilder blockBuilder)
        {
            int[] positionArray = positions.elements();
            if (block.mayHaveNull()) {
                for (int i = 0; i < positions.size(); i++) {
                    int position = positionArray[i];
                    if (block.isNull(position)) {
                        blockBuilder.appendNull();
                    }
                    else {
                        blockBuilder.writeShort(block.getShort(position, 0)).closeEntry();
                    }
                }
            }
            else {
                for (int i = 0; i < positions.size(); i++) {
                    blockBuilder.writeShort(block.getShort(positionArray[i], 0)).closeEntry();
                }
            }
        }

        @Override
        protected void appendDictionary(IntArrayList positions, DictionaryBlock block, BlockBuilder blockBuilder)
        {
            int[] positionArray = positions.elements();
            if (block.mayHaveNull()) {
                for (int i = 0; i < positions.size(); i++) {
                    int position = positionArray[i];
                    if (block.isNull(position)) {
                        blockBuilder.appendNull();
                    }
                    else {
                        blockBuilder.writeShort(block.getShort(position, 0)).closeEntry();
                    }
                }
            }
            else {
                for (int i = 0; i < positions.size(); i++) {
                    blockBuilder.writeShort(block.getShort(positionArray[i], 0)).closeEntry();
                }
            }
        }

        @Override
        protected void appendRle(int positionCount, Block block, int sourcePosition, BlockBuilder blockBuilder)
        {
            if (block.isNull(sourcePosition)) {
                for (int i = 0; i < positionCount; i++) {
                    blockBuilder.appendNull();
                }
            }
            else {
                short value = block.getShort(sourcePosition, 0);
                for (int i = 0; i < positionCount; i++) {
                    blockBuilder.writeShort(value).closeEntry();
                }
            }
        }

        @Override
        public PositionsAppender newState(BlockBuilderStatus blockBuilderStatus, int expectedPositions)
        {
            return new SmallintPositionsAppender(type, equalOperator, blockBuilderStatus, expectedPositions);
        }
    }

    public static class Int96PositionsAppender
            extends BlockBuilderPositionsAppender
    {
        public Int96PositionsAppender(Type type, BlockPositionEqual equalOperator, BlockBuilderStatus blockBuilderStatus, int expectedPositions)
        {
            super(type, equalOperator, blockBuilderStatus, expectedPositions);
        }

        @Override
        public void appendTo(IntArrayList positions, Block block, BlockBuilder blockBuilder)
        {
            int[] positionArray = positions.elements();
            if (block.mayHaveNull()) {
                for (int i = 0; i < positions.size(); i++) {
                    int position = positionArray[i];
                    if (block.isNull(position)) {
                        blockBuilder.appendNull();
                    }
                    else {
                        blockBuilder.writeLong(block.getLong(position, 0));
                        blockBuilder.writeInt(block.getInt(position, SIZE_OF_LONG));
                        blockBuilder.closeEntry();
                    }
                }
            }
            else {
                for (int i = 0; i < positions.size(); i++) {
                    int position = positionArray[i];
                    blockBuilder.writeLong(block.getLong(position, 0));
                    blockBuilder.writeInt(block.getInt(position, SIZE_OF_LONG));
                    blockBuilder.closeEntry();
                }
            }
        }

        @Override
        protected void appendDictionary(IntArrayList positions, DictionaryBlock block, BlockBuilder blockBuilder)
        {
            int[] positionArray = positions.elements();
            if (block.mayHaveNull()) {
                for (int i = 0; i < positions.size(); i++) {
                    int position = positionArray[i];
                    if (block.isNull(position)) {
                        blockBuilder.appendNull();
                    }
                    else {
                        blockBuilder.writeLong(block.getLong(position, 0));
                        blockBuilder.writeInt(block.getInt(position, SIZE_OF_LONG));
                        blockBuilder.closeEntry();
                    }
                }
            }
            else {
                for (int i = 0; i < positions.size(); i++) {
                    int position = positionArray[i];
                    blockBuilder.writeLong(block.getLong(position, 0));
                    blockBuilder.writeInt(block.getInt(position, SIZE_OF_LONG));
                    blockBuilder.closeEntry();
                }
            }
        }

        @Override
        protected void appendRle(int positionCount, Block block, int sourcePosition, BlockBuilder blockBuilder)
        {
            if (block.isNull(sourcePosition)) {
                for (int i = 0; i < positionCount; i++) {
                    blockBuilder.appendNull();
                }
            }
            else {
                long high = block.getLong(sourcePosition, 0);
                int low = block.getInt(sourcePosition, SIZE_OF_LONG);
                for (int i = 0; i < positionCount; i++) {
                    blockBuilder.writeLong(high);
                    blockBuilder.writeInt(low);
                    blockBuilder.closeEntry();
                }
            }
        }

        @Override
        public PositionsAppender newState(BlockBuilderStatus blockBuilderStatus, int expectedPositions)
        {
            return new Int96PositionsAppender(type, equalOperator, blockBuilderStatus, expectedPositions);
        }
    }

    public static class Int128PositionsAppender
            extends BlockBuilderPositionsAppender
    {
        public Int128PositionsAppender(Type type, BlockPositionEqual equalOperator, BlockBuilderStatus blockBuilderStatus, int expectedPositions)
        {
            super(type, equalOperator, blockBuilderStatus, expectedPositions);
        }

        @Override
        public void appendTo(IntArrayList positions, Block block, BlockBuilder blockBuilder)
        {
            int[] positionArray = positions.elements();
            if (block.mayHaveNull()) {
                for (int i = 0; i < positions.size(); i++) {
                    int position = positionArray[i];
                    if (block.isNull(position)) {
                        blockBuilder.appendNull();
                    }
                    else {
                        blockBuilder.writeLong(block.getLong(position, 0));
                        blockBuilder.writeLong(block.getLong(position, SIZE_OF_LONG));
                        blockBuilder.closeEntry();
                    }
                }
            }
            else {
                for (int i = 0; i < positions.size(); i++) {
                    int position = positionArray[i];
                    blockBuilder.writeLong(block.getLong(position, 0));
                    blockBuilder.writeLong(block.getLong(position, SIZE_OF_LONG));
                    blockBuilder.closeEntry();
                }
            }
        }

        @Override
        protected void appendDictionary(IntArrayList positions, DictionaryBlock block, BlockBuilder blockBuilder)
        {
            int[] positionArray = positions.elements();
            if (block.mayHaveNull()) {
                for (int i = 0; i < positions.size(); i++) {
                    int position = positionArray[i];
                    if (block.isNull(position)) {
                        blockBuilder.appendNull();
                    }
                    else {
                        blockBuilder.writeLong(block.getLong(position, 0));
                        blockBuilder.writeLong(block.getLong(position, SIZE_OF_LONG));
                        blockBuilder.closeEntry();
                    }
                }
            }
            else {
                for (int i = 0; i < positions.size(); i++) {
                    int position = positionArray[i];
                    blockBuilder.writeLong(block.getLong(position, 0));
                    blockBuilder.writeLong(block.getLong(position, SIZE_OF_LONG));
                    blockBuilder.closeEntry();
                }
            }
        }

        @Override
        protected void appendRle(int positionCount, Block block, int sourcePosition, BlockBuilder blockBuilder)
        {
            if (block.isNull(sourcePosition)) {
                for (int i = 0; i < positionCount; i++) {
                    blockBuilder.appendNull();
                }
            }
            else {
                long high = block.getLong(sourcePosition, 0);
                long low = block.getLong(sourcePosition, SIZE_OF_LONG);
                for (int i = 0; i < positionCount; i++) {
                    blockBuilder.writeLong(high);
                    blockBuilder.writeLong(low);
                    blockBuilder.closeEntry();
                }
            }
        }

        @Override
        public PositionsAppender newState(BlockBuilderStatus blockBuilderStatus, int expectedPositions)
        {
            return new Int128PositionsAppender(type, equalOperator, blockBuilderStatus, expectedPositions);
        }
    }
}

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
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.BlockBuilderStatus;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.type.Type;
import io.trino.type.BlockTypeOperators.BlockPositionEqual;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.openjdk.jol.info.ClassLayout;

import static java.util.Objects.requireNonNull;

public abstract class BlockBuilderPositionsAppender
        implements PositionsAppender
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(BlockBuilderPositionsAppender.class).instanceSize();

    protected final Type type;
    protected final BlockPositionEqual equalOperator;
    private final BlockBuilder blockBuilder;

    BlockBuilderPositionsAppender(Type type, BlockPositionEqual equalOperator, BlockBuilderStatus blockBuilderStatus, int expectedPositions)
    {
        this.type = requireNonNull(type, "type is null");
        this.blockBuilder = type.createBlockBuilder(blockBuilderStatus, expectedPositions);
        this.equalOperator = requireNonNull(equalOperator, "equalOperator is null");
    }

    public static class TypeBlockBuilderPositionsAppender
            extends BlockBuilderPositionsAppender
    {
        TypeBlockBuilderPositionsAppender(Type type, BlockPositionEqual equalOperator, BlockBuilderStatus blockBuilderStatus, int expectedPositions)
        {
            super(type, equalOperator, blockBuilderStatus, expectedPositions);
        }

        @Override
        protected void appendTo(IntArrayList positions, Block source, BlockBuilder blockBuilder)
        {
            int[] positionArray = positions.elements();
            for (int i = 0; i < positions.size(); i++) {
                type.appendTo(source, positionArray[i], blockBuilder);
            }
        }

        @Override
        protected void appendRle(int positionCount, Block source, int sourcePosition, BlockBuilder blockBuilder)
        {
            for (int i = 0; i < positionCount; i++) {
                type.appendTo(source, sourcePosition, blockBuilder);
            }
        }

        @Override
        public void appendDictionary(IntArrayList positions, DictionaryBlock source, BlockBuilder blockBuilder)
        {
            int[] positionArray = positions.elements();
            for (int i = 0; i < positions.size(); i++) {
                type.appendTo(source, positionArray[i], blockBuilder);
            }
        }

        @Override
        public PositionsAppender newState(BlockBuilderStatus blockBuilderStatus, int expectedPositions)
        {
            return new TypeBlockBuilderPositionsAppender(type, equalOperator, blockBuilderStatus, expectedPositions);
        }
    }

    @Override
    public void appendRle(int positionCount, Block source, int sourcePosition)
    {
        appendRle(positionCount, source, sourcePosition, blockBuilder);
    }

    protected abstract void appendRle(int positionCount, Block source, int sourcePosition, BlockBuilder blockBuilder);

    @Override
    public void appendTo(IntArrayList positions, Block source)
    {
        appendTo(positions, source, blockBuilder);
    }

    protected abstract void appendTo(IntArrayList positions, Block source, BlockBuilder blockBuilder);

    @Override
    public void appendDictionary(IntArrayList positions, DictionaryBlock source)
    {
        appendDictionary(positions, source, blockBuilder);
    }

    protected abstract void appendDictionary(IntArrayList positions, DictionaryBlock source, BlockBuilder blockBuilder);

    @Override
    public boolean firstValueEquals(Block other, int otherPosition)
    {
        return equalOperator.equalNullSafe(blockBuilder, 0, other, otherPosition);
    }

    @Override
    public Block build()
    {
        return blockBuilder.build();
    }

    @Override
    public void prepareProcessingByRow()
    {
    }

    @Override
    public void appendRow(Block source, int position)
    {
        type.appendTo(source, position, blockBuilder);
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + blockBuilder.getRetainedSizeInBytes();
    }
}

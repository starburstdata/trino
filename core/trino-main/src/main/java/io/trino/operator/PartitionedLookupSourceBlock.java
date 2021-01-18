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

import io.trino.spi.block.Block;
import io.trino.spi.type.Type;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class PartitionedLookupSourceBlock
        extends AbstractIndexBlock
{
    private final int partitionMask;
    private final int shiftSize;
    private final List<Block> partitionBlocks;
    private final Type type;

    public PartitionedLookupSourceBlock(int partitionMask, int shiftSize, List<Block> partitionBlocks, Type type, long[] addresses, Optional<boolean[]> valueIsNull, int arrayOffset, int positionCount)
    {
        super(type, addresses, valueIsNull.orElse(null), arrayOffset, positionCount);
        this.partitionMask = partitionMask;
        this.shiftSize = shiftSize;
        this.partitionBlocks = requireNonNull(partitionBlocks, "partitionBlocks is null");
        this.type = requireNonNull(type, "type is null");
    }

    @Override
    protected Block createIndexBlock(long[] addresses, @Nullable boolean[] valueIsNull, int arrayOffset, int positionCount)
    {
        return new PartitionedLookupSourceBlock(partitionMask, shiftSize, partitionBlocks, type, addresses, Optional.ofNullable(valueIsNull), arrayOffset, positionCount);
    }

    @Override
    protected Block getBlock(long address)
    {
        return partitionBlocks.get(decodePartition(address));
    }

    @Override
    protected int getBlockPosition(long address)
    {
        return decodeJoinPosition(address);
    }

    @Override
    public List<Block> getChildren()
    {
        return partitionBlocks;
    }

    private int decodePartition(long partitionedJoinPosition)
    {
        //noinspection NumericCastThatLosesPrecision - partitionMask is an int
        return (int) (partitionedJoinPosition & partitionMask);
    }

    private int decodeJoinPosition(long partitionedJoinPosition)
    {
        return toIntExact(partitionedJoinPosition >>> shiftSize);
    }
}

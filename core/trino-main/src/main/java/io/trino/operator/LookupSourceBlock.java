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

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.operator.SyntheticAddress.decodePosition;
import static io.trino.operator.SyntheticAddress.decodeSliceIndex;
import static java.util.Objects.requireNonNull;

public class LookupSourceBlock
        extends AbstractIndexBlock
{
    private final List<Block> blocks;
    private final Type type;

    public LookupSourceBlock(List<Block> blocks, Type type, long[] addresses, Optional<boolean[]> valueIsNull, int arrayOffset, int positionCount)
    {
        super(type, addresses, valueIsNull.orElse(null), arrayOffset, positionCount);
        this.blocks = requireNonNull(blocks, "blocks is null");
        this.type = requireNonNull(type, "type is null");

        checkArgument(arrayOffset >= 0 && arrayOffset <= addresses.length, "arrayOffset is not valid");
        checkArgument(positionCount >= 0 && positionCount <= addresses.length - arrayOffset, "positionCount is not valid");
    }

    @Override
    protected Block createIndexBlock(long[] addresses, @Nullable boolean[] valueIsNull, int arrayOffset, int positionCount)
    {
        return new LookupSourceBlock(blocks, type, addresses, Optional.ofNullable(valueIsNull), arrayOffset, positionCount);
    }

    @Override
    protected Block getBlock(long address)
    {
        return blocks.get(decodeSliceIndex(address));
    }

    @Override
    protected int getBlockPosition(long address)
    {
        return decodePosition(address);
    }

    @Override
    public List<Block> getChildren()
    {
        return blocks;
    }
}

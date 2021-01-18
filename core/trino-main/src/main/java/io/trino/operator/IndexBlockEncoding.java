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

import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockEncoding;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.spi.type.FixedWidthType;
import io.trino.spi.type.Type;

import java.util.Optional;

public class IndexBlockEncoding
        implements BlockEncoding
{
    public static final String NAME = "INDEX";

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput input)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeBlock(BlockEncodingSerde blockEncodingSerde, SliceOutput sliceOutput, Block block)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<Block> replacementBlockForWrite(Block block)
    {
        /*if (isFixedWidthType(((AbstractIndexBlock) block).getType())) {
            return Optional.empty();
        }*/

        return Optional.of(block.copyRegion(0, block.getPositionCount()));
    }

    private boolean isFixedWidthType(Type type)
    {
        if (!(type instanceof FixedWidthType)) {
            return false;
        }

        FixedWidthType fixedWidthType = (FixedWidthType) type;
        return fixedWidthType.getFixedSize() == 8;
    }
}

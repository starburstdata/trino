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
package io.prestosql.execution.buffer;

import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockEncoding;
import io.prestosql.spi.block.BlockEncodingSerde;
import io.prestosql.spi.block.LongArrayBlock;

import static io.prestosql.spi.block.EncoderUtil.decodeNullBits;
import static io.prestosql.spi.block.EncoderUtil.encodeNullsAsBits;
import static io.prestosql.spi.block.LongArrayBlockEncoding.NAME;

public class LongArrayBlockToFixedWidthBlockEncoding
        implements BlockEncoding
{
    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public void writeBlock(BlockEncodingSerde blockEncodingSerde, SliceOutput sliceOutput, Block block)
    {
        LongArrayBlock longArrayBlock = (LongArrayBlock) block;

        sliceOutput.appendInt(Long.BYTES);
        sliceOutput.appendInt(longArrayBlock.getPositionCount());

        // write null bits 8 at a time
        encodeNullsAsBits(sliceOutput, longArrayBlock);

        Slice slice = longArrayBlock.getRawSlice();
        sliceOutput
                .appendInt(slice.length())
                .writeBytes(slice);
    }

    @Override
    public Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        int fixedSize = sliceInput.readInt();
        int positionCount = sliceInput.readInt();

        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount)
                .orElse(null);

        int blockSize = sliceInput.readInt();
        Slice slice = sliceInput.readSlice(blockSize);

        return new FixedWidthBlock(fixedSize, positionCount, slice, valueIsNull, NAME);
    }
}

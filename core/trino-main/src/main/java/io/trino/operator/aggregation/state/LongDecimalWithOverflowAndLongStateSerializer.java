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
package io.trino.operator.aggregation.state;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.type.Int128;
import io.trino.spi.type.Type;

import static io.trino.spi.type.VarbinaryType.VARBINARY;

public class LongDecimalWithOverflowAndLongStateSerializer
        implements AccumulatorStateSerializer<LongDecimalWithOverflowAndLongState>
{
    private static final int SERIALIZED_SIZE = (Long.BYTES * 2) + Int128.SIZE;

    @Override
    public Type getSerializedType()
    {
        return VARBINARY;
    }

    @Override
    public void serialize(LongDecimalWithOverflowAndLongState state, BlockBuilder out)
    {
        if (state.isNotNull()) {
            long count = state.getLong();
            long overflow = state.getOverflow();
            long[] decimal = state.getDecimalArray();
            int offset = state.getDecimalArrayOffset();
            long[] buffer = new long[4];
            long decimalLowBytes = decimal[offset + 1];
            long decimalHighBytes = decimal[offset];
//            int bufferLength;
//            buffer[0] = decimalLowBytes;
//            if (decimalHighBytes != 0) {
//                buffer[1] = decimalHighBytes;
//                buffer[2] = overflow;
//                buffer[3] = count;
//                // if decimalHighBytes == 0 and count == 1 and overflow == 0 we only write decimalLowBytes (bufferLength = 1)
//                // if decimalHighBytes != 0 and count == 1 and overflow == 0 we write both decimalLowBytes and decimalHighBytes (bufferLength = 2)
//                // if count != 1 or overflow != 0 we write all values (bufferLength = 4)
//                bufferLength = (count == 1 & overflow == 0) ? 2 : 4;
//            }
//            else {
//                buffer[1] = count;
//                buffer[2] = overflow;
//                // if decimalHighBytes == 0 and count == 1 and overflow == 0 we only write decimalLowBytes (bufferLength = 1)
//                // if decimalHighBytes != 0 and count == 1 and overflow == 0 we write both decimalLowBytes and decimalHighBytes (bufferLength = 2)
//                // if count != 1 or overflow != 0 we write all values (bufferLength = 4)
//                bufferLength = (count == 1 & overflow == 0) ? 1 : 3;
//            }
            // append low
            buffer[0] = decimalLowBytes;
            // append high
            buffer[1] = decimalHighBytes;
            int countOffset = 1 + (decimalHighBytes == 0 ? 0 : 1);
            // append count, overflow
            buffer[countOffset] = count;
            buffer[countOffset + 1] = overflow;
            int bufferLength = countOffset + ((overflow == 0 & count == 1) ? 0 : 2);
            VARBINARY.writeSlice(out, Slices.wrappedLongArray(buffer, 0, bufferLength));
        }
        else {
            out.appendNull();
        }
    }

    @Override
    public void deserialize(Block block, int index, LongDecimalWithOverflowAndLongState state)
    {
        if (!block.isNull(index)) {
            Slice slice = VARBINARY.getSlice(block, index);
            long[] decimal = state.getDecimalArray();
            int offset = state.getDecimalArrayOffset();

            long low = slice.getLong(0);
            int sliceLength = slice.length();
            int highOffset = sliceLength == 4 * Long.BYTES | sliceLength == 2 * Long.BYTES ? Long.BYTES : 0;
            long high = slice.getLong(highOffset) & (highOffset != 0 ? -1L : 0);

            int countOffset = sliceLength > 2 * Long.BYTES ? highOffset + Long.BYTES : 0;
            long count = slice.getLong(countOffset);
            count = countOffset != 0 ? count : 1;
            int overflowOffset = sliceLength > 2 * Long.BYTES ? countOffset + Long.BYTES : 0;
            long overflow = slice.getLong(overflowOffset) & (overflowOffset != 0 ? -1L : 0);

            decimal[offset + 1] = low;
            decimal[offset] = high;
            state.setOverflow(overflow);
            state.setLong(count);
            state.setNotNull();
        }
    }
}

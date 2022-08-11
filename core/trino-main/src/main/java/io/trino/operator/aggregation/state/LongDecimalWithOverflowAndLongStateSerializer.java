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
            buffer[0] = decimal[offset + 1];
            buffer[1] = decimal[offset];
            buffer[2] = overflow;
            buffer[3] = count;
            int bufferLength = 1 + (decimal[offset] == 0 ? 0 : 1);
            bufferLength = (count == 1 & overflow == 0) ? bufferLength : 4;

            VARBINARY.writeSlice(out, Slices.wrappedLongArray(buffer, 0, bufferLength));
//            if (count == 1 && overflow == 0 && decimal[offset] == 0) {
//                // single decimal <= max long. just write low decimal
//                VARBINARY.writeSlice(out, Slices.wrappedLongArray(decimal[offset + 1]));
//            }
//            else {
//                VARBINARY.writeSlice(out, Slices.wrappedLongArray(count, overflow, decimal[offset], decimal[offset + 1]));
//            }
        }
        else {
            out.appendNull();
        }
    }

    @Override
    public void deserialize(Block block, int index, LongDecimalWithOverflowAndLongState state)
    {
        if (!block.isNull(index)) {
            state.setNotNull();
            Slice slice = VARBINARY.getSlice(block, index);
            long[] decimal = state.getDecimalArray();
            int offset = state.getDecimalArrayOffset();
            decimal[offset + 1] = slice.getLong(0);

//            long count = slice.length() == SERIALIZED_SIZE ? slice.getLong(Long.BYTES * 3) : 1;
//            long overflow = slice.length() == SERIALIZED_SIZE ? slice.getLong(Long.BYTES * 2) : 0;
//            long high = slice.length() > Long.BYTES ? slice.getLong(Long.BYTES) : 0;

            long count = 1;
            long overflow = 0;
            long high = 0;
            if (slice.length() > Long.BYTES) {
                high = slice.getLong(Long.BYTES);
                if (slice.length() == SERIALIZED_SIZE) {
                    overflow = slice.getLong(Long.BYTES * 2);
                    count = slice.getLong(Long.BYTES * 3);
                }
            }
            state.setLong(count);
            state.setOverflow(overflow);
            decimal[offset] = high;

//            if (slice.length() == Long.BYTES) {
//                state.setLong(1);
//                state.setOverflow(0);
//                decimal[offset] = 0;
//            }
//            else {
//                if (slice.length() != SERIALIZED_SIZE) {
//                    throw new IllegalStateException("Unexpected serialized state size: " + slice.length());
//                }
//
//                long count = slice.getLong(Long.BYTES * 3);
//                long overflow = slice.getLong(Long.BYTES * 2);
//
//                state.setLong(count);
//                state.setOverflow(overflow);
//                decimal[offset] = slice.getLong(Long.BYTES);
//
//            }
        }
    }
}

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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

import static io.trino.spi.type.VarbinaryType.VARBINARY;

public class VarHandleLongDecimalWithOverflowAndLongStateSerializer
        implements AccumulatorStateSerializer<LongDecimalWithOverflowAndLongState>
{
    private static final VarHandle LONG_ARRAY_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.nativeOrder());
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
            byte[] buffer = new byte[4 * Long.BYTES + 1];
            long decimalLowBytes = decimal[offset + 1];
            long decimalHighBytes = decimal[offset];
            // cases
            // high == 0 (countOffset = 1 * Long.BYTES)
            //    overflow != 0            -> bufferLength = 3 * Long.BYTES
            //    overflow == 0, count > 1 -> bufferLength = 2 * Long.BYTES + 1
            //    overflow == 0, count = 1 -> bufferLength = 1 * Long.BYTES
            // high != 0 (countOffset = 2 * Long.BYTES)
            //    overflow != 0            -> bufferLength = 4 * Long.BYTES
            //    overflow == 0, count > 1 -> bufferLength = 3 * Long.BYTES + 1
            //    overflow == 0, count = 1 -> bufferLength = 2 * Long.BYTES
            LONG_ARRAY_HANDLE.set(buffer, 0, decimalLowBytes);
            LONG_ARRAY_HANDLE.set(buffer, Long.BYTES, decimalHighBytes);
            int countOffset = (1 + (decimalHighBytes == 0 ? 0 : 1)) * Long.BYTES;
            LONG_ARRAY_HANDLE.set(buffer, countOffset, count);
            LONG_ARRAY_HANDLE.set(buffer, countOffset + Long.BYTES, overflow);
            boolean includeCount = count != 1 || overflow != 0;
            int bufferLength = countOffset + (includeCount ? Long.BYTES : 0) + (overflow != 0 ? Long.BYTES : 0) + (overflow == 0 & decimalHighBytes == 0 & count != 1 ? 1 : 0);
            VARBINARY.writeSlice(out, Slices.wrappedBuffer(buffer, 0, bufferLength));
        }
        else {
            out.appendNull();
        }
    }

    //    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    @Override
    public void deserialize(Block block, int index, LongDecimalWithOverflowAndLongState state)
    {
        if (!block.isNull(index)) {
            Slice slice = VARBINARY.getSlice(block, index);
            long[] decimal = state.getDecimalArray();
            int offset = state.getDecimalArrayOffset();

            long low = slice.getLong(0);
            int sliceLength = slice.length();
            int highOffset = sliceLength == 4 * Long.BYTES | sliceLength == 2 * Long.BYTES | sliceLength == 3 * Long.BYTES + 1 ? Long.BYTES : 0;
            long high = slice.getLong(highOffset) & (highOffset != 0 ? -1L : 0);

            int countOffset = sliceLength == 3 * Long.BYTES | sliceLength == 2 * Long.BYTES + 1 ? Long.BYTES : 2 * Long.BYTES;
            countOffset = sliceLength > 2 * Long.BYTES ? countOffset : 0;
            long count = slice.getLong(countOffset);
            count = countOffset != 0 ? count : 1;

            int overflowOffset = sliceLength == 3 * Long.BYTES ? 2 * Long.BYTES : 3 * Long.BYTES;
            overflowOffset = sliceLength == 3 * Long.BYTES | sliceLength == 4 * Long.BYTES ? overflowOffset : 0;
            long overflow = slice.getLong(overflowOffset) & (overflowOffset != 0 ? -1L : 0);

//            long high = 0;
//            long overflow = 0;
//            long count = 1;
//            if (sliceLength == 4 * Long.BYTES | sliceLength == 2 * Long.BYTES | sliceLength == 3 * Long.BYTES + 1) {
//                // high != 0
//                high = slice.getLong(Long.BYTES);
//                if (sliceLength == 4 * Long.BYTES) {
//                    overflow = slice.getLong(Long.BYTES * 3);
//                }
//                if (sliceLength != 2 * Long.BYTES) {
//                    count = slice.getLong(Long.BYTES * 2);
//                }
//            }
//            else {
//                // high == 0
//                if (sliceLength == 3 * Long.BYTES) {
//                    overflow = slice.getLong(Long.BYTES * 2);
//                }
//                if (sliceLength != Long.BYTES) {
//                    count = slice.getLong(Long.BYTES);
//                }
//            }

//            if (sliceLength == 4 * Long.BYTES) {
//                high = slice.getLong(Long.BYTES);
//                count = slice.getLong(Long.BYTES * 2);
//                overflow = slice.getLong(Long.BYTES * 3);
//            }
//            else if (sliceLength == 3 * Long.BYTES + 1) {
//                high = slice.getLong(Long.BYTES);
//                count = slice.getLong(Long.BYTES * 2);
//            }
//            else if (sliceLength == 2 * Long.BYTES) {
//                high = slice.getLong(Long.BYTES);
//            }
//            else if (sliceLength == 3 * Long.BYTES) {
//                count = slice.getLong(Long.BYTES);
//                overflow = slice.getLong(Long.BYTES * 2);
//            }
//            else if (sliceLength == 2 * Long.BYTES + 1) {
//                count = slice.getLong(Long.BYTES);
//            }

            decimal[offset + 1] = low;
            decimal[offset] = high;
            state.setOverflow(overflow);
            state.setLong(count);
            state.setNotNull();
        }
    }
}

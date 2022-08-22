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
            int bufferLength;
            LONG_ARRAY_HANDLE.set(buffer, 0, decimalLowBytes);

            if (decimalHighBytes != 0) {
                LONG_ARRAY_HANDLE.set(buffer, Long.BYTES, decimalHighBytes);
                LONG_ARRAY_HANDLE.set(buffer, Long.BYTES * 2, count);
                LONG_ARRAY_HANDLE.set(buffer, Long.BYTES * 3, overflow);
                // if decimalHighBytes == 0 and count == 1 and overflow == 0 we only write decimalLowBytes (bufferLength = 1)
                // if decimalHighBytes != 0 and count == 1 and overflow == 0 we write both decimalLowBytes and decimalHighBytes (bufferLength = 2)
                // if count != 1 or overflow != 0 we write all values (bufferLength = 4)
                bufferLength = (count == 1 & overflow == 0) ? 2 * Long.BYTES : 4 * Long.BYTES;
            }
            else {
                LONG_ARRAY_HANDLE.set(buffer, Long.BYTES, count);
                LONG_ARRAY_HANDLE.set(buffer, Long.BYTES * 2, overflow);

                // if decimalHighBytes == 0 and count == 1 and overflow == 0 we only write decimalLowBytes (bufferLength = 1)
                // if decimalHighBytes != 0 and count == 1 and overflow == 0 we write both decimalLowBytes and decimalHighBytes (bufferLength = 2)
                // if count != 1 or overflow != 0 we write all values (bufferLength = 4)
                if (overflow != 0) {
                    bufferLength = 3 * Long.BYTES;
                }
                else {
                    bufferLength = (count == 1) ? Long.BYTES : 2 * Long.BYTES + 1; // bufferLength = 17 means both decimalHighBytes and overflow == 0
                }
            }

            VARBINARY.writeSlice(out, Slices.wrappedBuffer(buffer, 0, bufferLength));
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

            long high = 0;
            long overflow = 0;
            long count = 1;
            if (slice.length() > Long.BYTES) {
                high = slice.getLong(Long.BYTES);
                if (slice.length() == SERIALIZED_SIZE) {
                    overflow = slice.getLong(Long.BYTES * 2);
                    count = slice.getLong(Long.BYTES * 3);
                }
            }

            decimal[offset + 1] = slice.getLong(0);
            decimal[offset] = high;
            state.setOverflow(overflow);
            state.setLong(count);
            state.setNotNull();
        }
    }
}

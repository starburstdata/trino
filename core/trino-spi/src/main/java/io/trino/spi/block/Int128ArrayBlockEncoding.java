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
package io.trino.spi.block;

import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;

import static io.trino.spi.block.EncoderUtil.decodeNullBits;
import static io.trino.spi.block.EncoderUtil.encodeNullsAsBits;

public class Int128ArrayBlockEncoding
        implements BlockEncoding
{
    public static final String NAME = "INT128_ARRAY";

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public void writeBlock(BlockEncodingSerde blockEncodingSerde, SliceOutput sliceOutput, Block block)
    {
        int positionCount = block.getPositionCount();
        sliceOutput.appendInt(positionCount);

        encodeNullsAsBits(sliceOutput, block);

        boolean nonZeroHigh = false;
        for (int i = 0; i < positionCount; i++) {
            if (block.getLong(i, 0) != 0) {
                nonZeroHigh = true;
                break;
            }
        }

        sliceOutput.writeBoolean(nonZeroHigh);
        if (!block.mayHaveNull()) {
            if (nonZeroHigh) {
                sliceOutput.writeBytes(getValuesSlice(block));
            }
            else {
                long[] low = new long[positionCount];
                for (int i = 0; i < positionCount; i++) {
                    low[i] = block.getLong(i, 8);
                }
                sliceOutput.writeBytes(Slices.wrappedLongArray(low));
            }
        }
        else {
            if (nonZeroHigh) {
                long[] valuesWithoutNull = new long[positionCount * 2];
                int nonNullPositionCount = 0;
                for (int i = 0; i < positionCount; i++) {
                    valuesWithoutNull[nonNullPositionCount] = block.getLong(i, 0);
                    valuesWithoutNull[nonNullPositionCount + 1] = block.getLong(i, 8);
                    if (!block.isNull(i)) {
                        nonNullPositionCount += 2;
                    }
                }

                sliceOutput.writeInt(nonNullPositionCount / 2);

                sliceOutput.writeBytes(Slices.wrappedLongArray(valuesWithoutNull, 0, nonNullPositionCount));
            }
            else {
                long[] valuesWithoutNull = new long[positionCount];
                int nonNullPositionCount = 0;
                for (int i = 0; i < positionCount; i++) {
                    valuesWithoutNull[nonNullPositionCount] = block.getLong(i, 8);
                    nonNullPositionCount = block.isNull(i) ? nonNullPositionCount : nonNullPositionCount + 1;
                }

                sliceOutput.writeInt(nonNullPositionCount);

                sliceOutput.writeBytes(Slices.wrappedLongArray(valuesWithoutNull, 0, nonNullPositionCount));
            }
        }
    }

    @Override
    public Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        int positionCount = sliceInput.readInt();

        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount).orElse(null);
        boolean nonZeroHigh = sliceInput.readBoolean();

        long[] values = new long[positionCount * 2];
        if (valueIsNull == null) {
            if (nonZeroHigh) {
                sliceInput.readBytes(Slices.wrappedLongArray(values));
            }
            else {
                long[] low = new long[positionCount];
                sliceInput.readBytes(Slices.wrappedLongArray(low));
                for (int i = 0; i < positionCount; i++) {
                    values[i * 2 + 1] = low[i];
                }
            }
        }
        else {
            int nonNullPositionCount = sliceInput.readInt();
            if (nonZeroHigh) {
                sliceInput.readBytes(Slices.wrappedLongArray(values, 0, nonNullPositionCount * 2));
                int position = 2 * (nonNullPositionCount - 1);
                for (int i = positionCount - 1; i >= 0 && position >= 0; i--) {
                    System.arraycopy(values, position, values, 2 * i, 2);
                    if (!valueIsNull[i]) {
                        position -= 2;
                    }
                }
            }
            else {
                long[] low = new long[nonNullPositionCount];
                sliceInput.readBytes(Slices.wrappedLongArray(low, 0, nonNullPositionCount));
                int lowOffset = 0;
                for (int i = 0; i < positionCount; i++) {
                    if (!valueIsNull[i]) {
                        values[i * 2 + 1] = low[lowOffset++];
                    }
                }
            }
        }

        return new Int128ArrayBlock(0, positionCount, valueIsNull, values);
    }

    private Slice getValuesSlice(Block block)
    {
        if (block instanceof Int128ArrayBlock) {
            return ((Int128ArrayBlock) block).getValuesSlice();
        }
        else if (block instanceof Int128ArrayBlockBuilder) {
            return ((Int128ArrayBlockBuilder) block).getValuesSlice();
        }

        throw new IllegalArgumentException("Unexpected block type " + block.getClass().getSimpleName());
    }
}

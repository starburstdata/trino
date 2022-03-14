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
import io.trino.spi.block.BlockBuilderStatus;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.SingleRowBlock;
import io.trino.spi.type.RowType;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.spi.block.BlockUtil.calculateNewArraySize;
import static io.trino.spi.block.RowBlock.createRowBlockInternal;
import static java.util.Objects.requireNonNull;

public class RowPositionsAppender
        implements PositionsAppender
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(RowPositionsAppender.class).instanceSize();

    private final RowType type;
    private final PositionsAppender[] fields;
    @Nullable private final BlockBuilderStatus blockBuilderStatus;

    private int positionCount;
    private boolean hasNullRow;
    private boolean[] rowIsNull;
    private int[] fieldBlockOffsets;
    @Nullable
    private int[] tempBuffer;

    public static RowPositionsAppender createRowAppender(
            PositionsAppenderFactory positionsAppenderFactory,
            RowType type,
            @Nullable BlockBuilderStatus blockBuilderStatus,
            int expectedPositions)
    {
        PositionsAppender[] fields = new PositionsAppender[type.getFields().size()];
        for (int i = 0; i < fields.length; i++) {
            fields[i] = positionsAppenderFactory.create(type.getFields().get(i).getType(), blockBuilderStatus, expectedPositions);
        }
        return new RowPositionsAppender(type, fields, blockBuilderStatus, expectedPositions);
    }

    RowPositionsAppender(
            RowType type,
            PositionsAppender[] fields,
            @Nullable BlockBuilderStatus blockBuilderStatus,
            int expectedPositions)
    {
        this.type = requireNonNull(type, "type is null");
        this.fields = requireNonNull(fields, "fields is null");
        this.blockBuilderStatus = blockBuilderStatus;
        checkArgument(type.getFields().size() == fields.length);
        this.fieldBlockOffsets = new int[expectedPositions + 1];
        this.rowIsNull = new boolean[expectedPositions];
    }

    @Override
    public void appendTo(IntArrayList positions, Block source)
    {
        ensureCapacity(positions.size());
        RowBlock sourceRowBlock = (RowBlock) source;
        IntArrayList notNullPositions;
        if (sourceRowBlock.getFieldBlockOffsets() != null) {
            notNullPositions = getNotNullPositions(positions, sourceRowBlock);
        }
        else {
            // the source Block does not have nulls
            if (sourceRowBlock.getOffsetBase() != 0) {
                notNullPositions = offsetPositions(positions, sourceRowBlock.getOffsetBase());
            }
            else {
                notNullPositions = positions;
            }

            for (int i = 0; i < positions.size(); i++) {
                this.fieldBlockOffsets[positionCount + 1] = this.fieldBlockOffsets[positionCount] + 1;
                positionCount++;
            }
        }

        Block[] rawFieldBlocks = sourceRowBlock.getRawFieldBlocks();
        for (int i = 0; i < fields.length; i++) {
            fields[i].appendTo(notNullPositions, rawFieldBlocks[i]);
        }
    }

    private IntArrayList offsetPositions(IntArrayList positions, int offsetBase)
    {
        int[] notNullPositions = getTempBuffer(positions.size());
        for (int i = 0; i < positions.size(); i++) {
            notNullPositions[i] = offsetBase + positions.getInt(i);
        }
        return IntArrayList.wrap(notNullPositions);
    }

    private IntArrayList getNotNullPositions(IntArrayList positions, RowBlock sourceRowBlock)
    {
        int[] fieldBlockOffsets = sourceRowBlock.getFieldBlockOffsets();
        checkArgument(fieldBlockOffsets != null);

        int offsetBase = sourceRowBlock.getOffsetBase();
        int[] notNullPositionsArray = getTempBuffer(positions.size());
        int notNullPositionsCount = 0;
        for (int i = 0; i < positions.size(); i++) {
            int position = positions.getInt(i);
            boolean positionIsNull = sourceRowBlock.isNull(position);

            byte increment = (byte) (positionIsNull ? 0 : 1);
            notNullPositionsArray[notNullPositionsCount] = fieldBlockOffsets[offsetBase + position];
            notNullPositionsCount += increment;

            rowIsNull[positionCount] = positionIsNull;
            hasNullRow |= positionIsNull;
            this.fieldBlockOffsets[positionCount + 1] = this.fieldBlockOffsets[positionCount] + increment;
            positionCount++;
        }
        return IntArrayList.wrap(notNullPositionsArray, notNullPositionsCount);
    }

    @Override
    public void appendRle(int positionCount, Block source, int sourcePosition)
    {
        ensureCapacity(positionCount);
        RowBlock sourceRowBlock = (RowBlock) source;
        if (source.isNull(sourcePosition)) {
            // append positionCount nulls
            hasNullRow = true;
            int fieldBlockOffset = fieldBlockOffsets[this.positionCount];
            for (; this.positionCount < this.positionCount + positionCount; this.positionCount++) {
                rowIsNull[this.positionCount] = true;
                this.fieldBlockOffsets[this.positionCount + 1] = fieldBlockOffset;
            }
        }
        else {
            // append not null row value
            int fieldBlockOffset = sourceRowBlock.getFieldBlockOffsets()[sourceRowBlock.getOffsetBase() + sourcePosition];
            Block[] rawFieldBlocks = sourceRowBlock.getRawFieldBlocks();
            for (int i = 0; i < fields.length; i++) {
                fields[i].appendRle(positionCount, rawFieldBlocks[i], fieldBlockOffset);
            }
            for (; this.positionCount < this.positionCount + positionCount; this.positionCount++) {
                this.fieldBlockOffsets[this.positionCount + 1] = this.fieldBlockOffsets[this.positionCount] + 1;
            }
        }
    }

    @Override
    public void appendDictionary(IntArrayList positions, DictionaryBlock source)
    {
        ensureCapacity(positions.size());
        RowBlock sourceRowBlock = (RowBlock) source.getDictionary();
        int[] fieldBlockOffsets = sourceRowBlock.getFieldBlockOffsets();
        IntArrayList notNullPositions = IntArrayList.wrap(getTempBuffer(positions.size()), 0);
        if (fieldBlockOffsets != null) {
            int offsetBase = sourceRowBlock.getOffsetBase();
            for (int i = 0; i < positions.size(); i++) {
                int position = positions.getInt(i);
                boolean positionIsNull = source.isNull(position);
                if (positionIsNull) {
                    rowIsNull[positionCount] = true;
                    hasNullRow = true;
                }
                else {
                    notNullPositions.add(fieldBlockOffsets[offsetBase + source.getId(position)]);
                }

                this.fieldBlockOffsets[positionCount + 1] = this.fieldBlockOffsets[positionCount] + (positionIsNull ? 0 : 1);
                positionCount++;
            }
        }
        else {
            // the source Block does not have nulls, remap dictionary ids only
            for (int i = 0; i < positions.size(); i++) {
                notNullPositions.add(source.getId(positions.getInt(i)));
                this.fieldBlockOffsets[positionCount + 1] = this.fieldBlockOffsets[positionCount] + 1;
                positionCount++;
            }
        }

        Block[] rawFieldBlocks = sourceRowBlock.getRawFieldBlocks();
        for (int i = 0; i < fields.length; i++) {
            fields[i].appendTo(notNullPositions, rawFieldBlocks[i]);
        }
    }

    private int[] getTempBuffer(int length)
    {
        if (tempBuffer == null || tempBuffer.length < length) {
            tempBuffer = new int[length];
        }
        return tempBuffer;
    }

    private void ensureCapacity(int additionalCapacity)
    {
        if (rowIsNull.length <= positionCount + additionalCapacity) {
            // grow by at least 50 %
            int newCapacity = Math.max(calculateNewArraySize(rowIsNull.length), positionCount + additionalCapacity);
            rowIsNull = Arrays.copyOf(rowIsNull, newCapacity);
            fieldBlockOffsets = Arrays.copyOf(fieldBlockOffsets, newCapacity + 1);
        }
    }

    @Override
    public Block build()
    {
        Block[] fieldBlocks = new Block[fields.length];
        for (int i = 0; i < fields.length; i++) {
            fieldBlocks[i] = fields[i].build();
        }

        return createRowBlockInternal(0, positionCount, hasNullRow ? rowIsNull : null, hasNullRow ? fieldBlockOffsets : null, fieldBlocks);
    }

    @Override
    public boolean firstValueEquals(Block other, int otherPosition)
    {
        boolean thisIsNull = rowIsNull[0];
        boolean otherIsNull = other.isNull(otherPosition);
        if (thisIsNull || otherIsNull) {
            return thisIsNull && otherIsNull;
        }

        if (!type.isComparable()) {
            return false;
        }

        // first flatten DictionaryBlock and RunLengthEncodedBlock
        while (other instanceof DictionaryBlock || other instanceof RunLengthEncodedBlock) {
            if (other instanceof DictionaryBlock) {
                DictionaryBlock dictionaryBlock = (DictionaryBlock) other;
                other = dictionaryBlock.getDictionary();
                otherPosition = dictionaryBlock.getId(otherPosition);
            }
            else {
                other = ((RunLengthEncodedBlock) other).getValue();
                otherPosition = 0;
            }
        }
        if (!(other instanceof RowBlock)) {
            // should not happen. TODO lysy: should we throw here? worst case if we don't throw is we lose RLE support for this case
            return false;
        }
        RowBlock otherRowBlock = (RowBlock) other;
        SingleRowBlock singleRowBlock = otherRowBlock.getObject(otherPosition, SingleRowBlock.class);
        for (int i = 0; i < fields.length; i++) {
            if (!fields[i].firstValueEquals(singleRowBlock.getRawFieldBlock(i), singleRowBlock.getRowIndex())) {
                return false;
            }
        }
        return false;
    }

    @Override
    public PositionsAppender newState(BlockBuilderStatus blockBuilderStatus, int expectedPositions)
    {
        PositionsAppender[] newFields = new PositionsAppender[fields.length];
        for (int i = 0; i < fields.length; i++) {
            newFields[i] = fields[i].newState(blockBuilderStatus, expectedPositions);
        }
        return new RowPositionsAppender(type, newFields, blockBuilderStatus, expectedPositions);
    }

    @Override
    public void prepareProcessingByRow()
    {
        for (PositionsAppender field : fields) {
            field.prepareProcessingByRow();
        }
    }

    @Override
    public void appendRow(Block block, int position)
    {
        ensureCapacity(1);
        if (block.isNull(position)) {
            appendNull();
        }
        else {
            writeObject(type.getObject(block, position));
        }
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        long size = INSTANCE_SIZE + sizeOf(fieldBlockOffsets) + sizeOf(rowIsNull);
        for (PositionsAppender field : fields) {
            size += field.getRetainedSizeInBytes();
        }
        if (blockBuilderStatus != null) {
            size += BlockBuilderStatus.INSTANCE_SIZE;
        }
        return size;
    }

    private void appendNull()
    {
        rowIsNull[positionCount] = true;
        hasNullRow = true;
        this.fieldBlockOffsets[positionCount + 1] = this.fieldBlockOffsets[positionCount];
        entryAdded();
    }

    private void writeObject(Block rowBlock)
    {
        for (int i = 0; i < rowBlock.getPositionCount(); i++) {
            fields[i].appendRow(rowBlock, i);
        }
        this.fieldBlockOffsets[positionCount + 1] = this.fieldBlockOffsets[positionCount] + 1;
        entryAdded();
    }

    private void entryAdded()
    {
        positionCount++;
        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(Integer.BYTES + Byte.BYTES);
        }
    }
}

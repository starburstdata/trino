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
import io.trino.spi.block.RunLengthEncodedBlock;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.commons.math3.util.Pair;
import org.openjdk.jol.info.ClassLayout;

import static java.util.Objects.requireNonNull;

public class BlockTypeDispatchingPositionsAppender
        implements PositionsAppender
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(BlockTypeDispatchingPositionsAppender.class).instanceSize();

    private final PositionsAppender delegate;

    public BlockTypeDispatchingPositionsAppender(PositionsAppender delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public void appendTo(IntArrayList positions, Block source)
    {
        if (positions.isEmpty()) {
            return;
        }

        // TODO lysy: test flatten
        if (source instanceof RunLengthEncodedBlock) {
            delegate.appendRle(positions.size(), flattenRle((RunLengthEncodedBlock) source), 0);
        }
        else if (source instanceof DictionaryBlock) {
            Pair<Block, IntArrayList> flatDictionary = flattenDictionary((DictionaryBlock) source, positions);
            if (flatDictionary.getKey() instanceof DictionaryBlock) {
                delegate.appendDictionary(flatDictionary.getValue(), (DictionaryBlock) flatDictionary.getKey());
            }
            else {
                delegate.appendRle(positions.size(), flatDictionary.getKey(), 0);
            }
        }
        else {
            delegate.appendTo(positions, source);
        }
    }

    private Pair<Block, IntArrayList> flattenDictionary(DictionaryBlock source, IntArrayList positions)
    {
        Block dictionary = source.getDictionary();
        if (!(dictionary instanceof RunLengthEncodedBlock || dictionary instanceof DictionaryBlock)) {
            return Pair.create(source, positions);
        }

        while (dictionary instanceof RunLengthEncodedBlock || dictionary instanceof DictionaryBlock) {
            if (dictionary instanceof RunLengthEncodedBlock) {
                // since at some level, dictionary contains only a single value, it can be flattened to rle
                RunLengthEncodedBlock rleDictionary = flattenRle((RunLengthEncodedBlock) dictionary);
                return Pair.create(new RunLengthEncodedBlock(rleDictionary.getValue(), Integer.MAX_VALUE), positions);
            }
            else {
                // dictionary is a nested dictionary. we need to remap the ids
                DictionaryBlock dictionaryValue = (DictionaryBlock) dictionary;
                int[] newPositions = new int[positions.size()];
                for (int i = 0; i < newPositions.length; i++) {
                    newPositions[i] = source.getId(positions.getInt(i));
                }
                positions = IntArrayList.wrap(newPositions);
                dictionary = dictionaryValue.getDictionary();
                source = dictionaryValue;
            }
        }
        return Pair.create(source, positions);
    }

    private RunLengthEncodedBlock flattenRle(RunLengthEncodedBlock source)
    {
        Block value = source.getValue();
        if (!(value instanceof RunLengthEncodedBlock || value instanceof DictionaryBlock)) {
            return source;
        }

        int positionCount = source.getPositionCount();
        int position = 0;
        while (value instanceof RunLengthEncodedBlock || value instanceof DictionaryBlock) {
            if (value instanceof RunLengthEncodedBlock) {
                value = ((RunLengthEncodedBlock) value).getValue();
            }
            else {
                DictionaryBlock dictionaryValue = (DictionaryBlock) value;
                position = dictionaryValue.getId(position);
                value = dictionaryValue.getDictionary();
            }
        }

        if (value.getPositionCount() > 1 || position != 0) {
            value = value.getSingleValueBlock(position);
        }
        return new RunLengthEncodedBlock(value, positionCount);
    }

    @Override
    public void appendRle(int positionCount, Block source, int sourcePosition)
    {
        delegate.appendRle(positionCount, source, sourcePosition);
    }

    @Override
    public void appendDictionary(IntArrayList positions, DictionaryBlock source)
    {
        delegate.appendDictionary(positions, source);
    }

    @Override
    public boolean firstValueEquals(Block other, int otherPosition)
    {
        return delegate.firstValueEquals(other, otherPosition);
    }

    @Override
    public PositionsAppender newState(BlockBuilderStatus blockBuilderStatus, int expectedPositions)
    {
        return new BlockTypeDispatchingPositionsAppender(delegate.newState(blockBuilderStatus, expectedPositions));
    }

    @Override
    public void prepareProcessingByRow()
    {
        delegate.prepareProcessingByRow();
    }

    @Override
    public void appendRow(Block source, int position)
    {
        delegate.appendRow(source, position);
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + delegate.getRetainedSizeInBytes();
    }

    @Override
    public Block build()
    {
        return delegate.build();
    }
}

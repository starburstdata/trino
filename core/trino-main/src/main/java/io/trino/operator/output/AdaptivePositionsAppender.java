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
import org.openjdk.jol.info.ClassLayout;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class AdaptivePositionsAppender
        implements PositionsAppender
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(AdaptivePositionsAppender.class).instanceSize();

    private PositionsAppender current;
    // list of RLE blocks that were produced so far
    private final List<RunLengthEncodedBlock> rleBlocks = new ArrayList<>();
    private final BlockBuilderStatus blockBuilderStatus;
    private final int expectedPositions;

    // -1 means flat state, 0 means initial empty state, positive means rle state and the current rle position count.
    private int rlePositionCount;

    public AdaptivePositionsAppender(PositionsAppender current, BlockBuilderStatus blockBuilderStatus, int expectedPositions)
    {
        this.current = requireNonNull(current, "appender is null");
        this.blockBuilderStatus = blockBuilderStatus;
        this.expectedPositions = expectedPositions;
    }

    @Override
    public void appendTo(IntArrayList positions, Block source)
    {
        if (rlePositionCount > 0) {
            // we are in the rle state, flatten all RLE blocks
            flattenRle();
        }
        rlePositionCount = -1;
        current.appendTo(positions, source);
    }

    @Override
    public void appendDictionary(IntArrayList positions, DictionaryBlock source)
    {
        if (rlePositionCount > 0) {
            // we are in the rle state, flatten all RLE blocks
            flattenRle();
        }
        rlePositionCount = -1;
        current.appendDictionary(positions, source);
    }

    @Override
    public void appendRle(int positionCount, Block source, int sourcePosition)
    {
        if (rlePositionCount == 0) {
            // initial empty state, switch to rle state
            current = current.newState(blockBuilderStatus, 1);
            current.appendRle(1, source, sourcePosition);
            rlePositionCount = positionCount;
        }
        else if (rlePositionCount > 0) {
            // we are in the rle state
            if (current.firstValueEquals(source, sourcePosition)) {
                // the values match. we can just add positions.
                this.rlePositionCount += positionCount;
                return;
            }
            // RLE values do not match. let's save the current RLE and set the new one as current
            rleBlocks.add(new RunLengthEncodedBlock(current.build(), rlePositionCount));
            current = current.newState(blockBuilderStatus, 1);
            current.appendRle(1, source, sourcePosition);
            rlePositionCount = positionCount;
        }
        else {
            // flat state
            current.appendRle(positionCount, source, sourcePosition);
        }
    }

    @Override
    public boolean firstValueEquals(Block other, int otherPosition)
    {
        return current.firstValueEquals(other, otherPosition);
    }

    @Override
    public PositionsAppender newState(BlockBuilderStatus blockBuilderStatus, int expectedPositions)
    {
        return new AdaptivePositionsAppender(this.current.newState(blockBuilderStatus, expectedPositions), blockBuilderStatus, expectedPositions);
    }

    @Override
    public Block build()
    {
        Block lastBlock = current.build();
        if (rleBlocks.isEmpty()) {
            if (rlePositionCount > 0) {
                return new RunLengthEncodedBlock(lastBlock, rlePositionCount);
            }
            return lastBlock;
        }

        // if rleBlock are not empty, the following block must be also RLE
        // otherwise the earlier RLE blocks would be merged into an expanded Block
        // So we have multiple RLE blocks here.
        // TODO lysy: Currently, we expand all to normal Block, but it may be beneficial to create DictionaryBlock here
        checkArgument(rlePositionCount > 0);
        PositionsAppender expanded = current.newState(null, expectedPositions);
        for (RunLengthEncodedBlock rleBlock : rleBlocks) {
            expanded.appendRle(rleBlock);
        }

        expanded.appendRle(new RunLengthEncodedBlock(lastBlock, rlePositionCount));

        return expanded.build();
    }

    @Override
    public void prepareProcessingByRow()
    {
        if (rlePositionCount > 0) {
            // we are in the rle state, flatten all RLE blocks
            flattenRle();
        }
    }

    @Override
    public void appendRow(Block source, int position)
    {
        checkArgument(rlePositionCount <= 0);
        current.appendRow(source, position);
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        long retainedRleSize = rleBlocks.stream().mapToLong(RunLengthEncodedBlock::getRetainedSizeInBytes).sum();
        return INSTANCE_SIZE + retainedRleSize + current.getRetainedSizeInBytes();
    }

    private void flattenRle()
    {
        rleBlocks.add(new RunLengthEncodedBlock(current.build(), rlePositionCount));
        current = current.newState(blockBuilderStatus, expectedPositions);
        for (RunLengthEncodedBlock rleBlock : rleBlocks) {
            current.appendRle(rleBlock);
        }
        rleBlocks.clear();
        // mark current state as flat
        rlePositionCount = -1;
    }
}

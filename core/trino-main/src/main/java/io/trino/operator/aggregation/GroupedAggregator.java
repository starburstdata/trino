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
package io.trino.operator.aggregation;

import com.google.common.primitives.Ints;
import io.trino.operator.GroupByIdBlock;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.Type;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Step;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class GroupedAggregator
{
    private final GroupedAccumulator accumulator;
    private AggregationNode.Step step;
    private final Type intermediateType;
    private final Type finalType;
    private final int[] rawInputChannels;
    private final OptionalInt intermediateStateChannel;
    private final OptionalInt rawInputMaskChannel;
    private final OptionalInt maskChannel;

    public GroupedAggregator(
            GroupedAccumulator accumulator,
            Step step,
            Type intermediateType,
            Type finalType,
            List<Integer> rawInputChannels,
            OptionalInt intermediateStateChannel,
            OptionalInt rawInputMaskChannel,
            OptionalInt maskChannel)
    {
        this.accumulator = requireNonNull(accumulator, "accumulator is null");
        this.step = requireNonNull(step, "step is null");
        this.intermediateType = requireNonNull(intermediateType, "intermediateType is null");
        this.finalType = requireNonNull(finalType, "finalType is null");
        this.rawInputChannels = Ints.toArray(requireNonNull(rawInputChannels, "inputChannels is null"));
        this.intermediateStateChannel = requireNonNull(intermediateStateChannel, "intermediateStateChannel is null");
        this.rawInputMaskChannel = requireNonNull(rawInputMaskChannel, "rawInputMaskChannel is null");
        this.maskChannel = requireNonNull(maskChannel, "maskChannel is null");
        checkArgument(step.isInputRaw() || intermediateStateChannel.isPresent(), "expected intermediateStateChannel for intermediate aggregation but got %s ", intermediateStateChannel);
    }

    public long getEstimatedSize()
    {
        return accumulator.getEstimatedSize();
    }

    public Type getType()
    {
        if (step.isOutputPartial()) {
            return intermediateType;
        }
        else {
            return finalType;
        }
    }

    public void processPage(GroupByIdBlock groupIds, Page page)
    {
        if (step.isInputRaw()) {
            accumulator.addInput(groupIds, page.getColumns(rawInputChannels), getMaskBlock(page));
            return;
        }

        if (rawInputMaskChannel.isEmpty()) {
            // process partially aggregated data
            accumulator.addIntermediate(groupIds, page.getBlock(intermediateStateChannel.getAsInt()));
            return;
        }
        Block rawInputMaskBlock = page.getBlock(rawInputMaskChannel.getAsInt());
        if (rawInputMaskBlock instanceof RunLengthEncodedBlock) {
            if (rawInputMaskBlock.isNull(0)) {
                // process partially aggregated data
                accumulator.addIntermediate(groupIds, page.getBlock(intermediateStateChannel.getAsInt()));
            }
            else {
                // process raw data
                accumulator.addInput(groupIds, page.getColumns(rawInputChannels), getMaskBlock(page));
            }
            return;
        }

        // rawInputMaskBlock has potentially mixed partially aggregated and raw data
        Block maskBlock = getMaskBlock(page)
                .map(mask -> andMasks(mask, rawInputMaskBlock))
                .orElse(rawInputMaskBlock);
        accumulator.addInput(groupIds, page.getColumns(rawInputChannels), Optional.of(maskBlock));
        IntArrayList intermediatePositions = filterByNull(rawInputMaskBlock);
        Block intermediateStateBlock = page.getBlock(intermediateStateChannel.getAsInt());

        if (intermediatePositions.size() != rawInputMaskBlock.getPositionCount()) {
            // some rows were eliminated by the filter
            intermediateStateBlock = intermediateStateBlock.getPositions(intermediatePositions.elements(), 0, intermediatePositions.size());
            groupIds = new GroupByIdBlock(
                    groupIds.getGroupCount(),
                    groupIds.getPositions(intermediatePositions.elements(), 0, intermediatePositions.size()));
        }

        accumulator.addIntermediate(groupIds, intermediateStateBlock);
    }

    private Block andMasks(Block mask1, Block mask2)
    {
        int positionCount = mask1.getPositionCount();
        byte[] mask = new byte[positionCount];
        for (int i = 0; i < positionCount; i++) {
            mask[i] = (byte) ((!mask1.isNull(i) && mask1.getByte(i, 0) == 1 && !mask2.isNull(i) && mask2.getByte(i, 0) == 1) ? 1 : 0);
        }
        return new ByteArrayBlock(positionCount, Optional.empty(), mask);
    }

    private Optional<Block> getMaskBlock(Page page)
    {
        if (maskChannel.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(page.getBlock(maskChannel.getAsInt()));
    }

    public void prepareFinal()
    {
        accumulator.prepareFinal();
    }

    public void evaluate(int groupId, BlockBuilder output)
    {
        if (step.isOutputPartial()) {
            accumulator.evaluateIntermediate(groupId, output);
        }
        else {
            accumulator.evaluateFinal(groupId, output);
        }
    }

    // todo this should return a new GroupedAggregator instead of modifying the existing object
    public void setSpillOutput()
    {
        step = AggregationNode.Step.partialOutput(step);
    }

    public Type getSpillType()
    {
        return intermediateType;
    }

    private static IntArrayList filterByNull(Block mask)
    {
        int positions = mask.getPositionCount();

        IntArrayList ids = new IntArrayList(positions);
        for (int i = 0; i < positions; ++i) {
            if (mask.isNull(i)) {
                ids.add(i);
            }
        }

        return ids;
    }
}

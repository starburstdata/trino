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
package io.trino.operator.aggregation.partial;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.trino.operator.aggregation.AggregatorFactory;
import io.trino.operator.aggregation.GroupedAggregator;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static java.util.Objects.requireNonNull;

/**
 * Partial aggregation output has additional channels compared to final aggregation output.
 * Those channels are used to send raw input data when the partial aggregation is disabled.
 * The additional channels are:
 * - mask channels used by any aggregation
 * - every aggregation input channels that are not already in hash channels
 * - a boolean rawInputMask channel that contains decision which input, aggregated or raw, should be used for a given position.
 */
public class PartialAggregationOutputProcessor
{
    private final List<GroupedAggregator> groupedAggregators;
    private final List<Type> aggregationInputTypes;
    private final int[] hashChannels;
    private final int[] aggregationInputChannels;
    private final int[] maskBlockChannels;

    public static PartialAggregationOutputProcessor create(
            List<Integer> groupByChannels,
            Optional<Integer> hashChannel,
            List<AggregatorFactory> aggregatorFactories,
            List<? extends Type> aggregationInputTypes,
            List<Integer> aggregationInputChannels)
    {
        return new PartialAggregationOutputProcessor(
                groupByChannels,
                hashChannel,
                aggregatorFactories,
                aggregationInputTypes,
                aggregationInputChannels,
                ImmutableList.copyOf(aggregatorFactories.stream()
                        .map(AggregatorFactory::getMaskChannel)
                        .flatMapToInt(OptionalInt::stream)
                        .boxed()
                        .collect(toImmutableSet())));
    }

    public PartialAggregationOutputProcessor(
            List<Integer> groupByChannels,
            Optional<Integer> inputHashChannel,
            List<AggregatorFactory> aggregatorFactories,
            List<? extends Type> aggregationInputTypes,
            List<Integer> aggregationInputChannels,
            List<Integer> maskBlockChannels)
    {
        this.aggregationInputTypes = ImmutableList.copyOf(aggregationInputTypes);

        this.groupedAggregators = requireNonNull(aggregatorFactories, "aggregatorFactories is null")
                .stream()
                .map(AggregatorFactory::createGroupedAggregator)
                .collect(toImmutableList());
        this.aggregationInputChannels = Ints.toArray(aggregationInputChannels);
        this.maskBlockChannels = Ints.toArray(maskBlockChannels);
        this.hashChannels = new int[groupByChannels.size() + (inputHashChannel.isPresent() ? 1 : 0)];
        for (int i = 0; i < groupByChannels.size(); i++) {
            hashChannels[i] = groupByChannels.get(i);
        }
        inputHashChannel.ifPresent(channelIndex -> hashChannels[groupByChannels.size()] = channelIndex);
    }

    public Page processAggregatedPage(Page page)
    {
        Block[] finalPage = new Block[page.getChannelCount() + maskBlockChannels.length + aggregationInputTypes.size() + 1];
        for (int i = 0; i < page.getChannelCount(); i++) {
            finalPage[i] = page.getBlock(i);
        }
        int positionCount = page.getPositionCount();

        // mask channels
        for (int i = 0; i < maskBlockChannels.length; i++) {
            finalPage[page.getChannelCount() + i] = nullRle(BOOLEAN, positionCount);
        }
        // aggregation inputs
        for (int i = 0; i < aggregationInputTypes.size(); i++) {
            finalPage[page.getChannelCount() + maskBlockChannels.length + i] = nullRle(aggregationInputTypes.get(i), positionCount);
        }

        // use raw input mask channel
        finalPage[finalPage.length - 1] = nullRle(BOOLEAN, positionCount);
        return new Page(positionCount, finalPage);
    }

    public Page processRawInputPage(Page page)
    {
        Block[] outputBlocks = new Block[hashChannels.length + groupedAggregators.size() + maskBlockChannels.length + aggregationInputChannels.length + 1];
        int blockOffset = 0;
        for (int i = 0; i < hashChannels.length; i++, blockOffset++) {
            outputBlocks[blockOffset] = page.getBlock(hashChannels[i]);
        }
        for (int i = 0; i < groupedAggregators.size(); i++, blockOffset++) {
            outputBlocks[blockOffset] = nullRle(groupedAggregators.get(i).getType(), page.getPositionCount());
        }
        for (int i = 0; i < maskBlockChannels.length; i++, blockOffset++) {
            outputBlocks[blockOffset] = page.getBlock(maskBlockChannels[i]);
        }
        for (int i = 0; i < aggregationInputChannels.length; i++, blockOffset++) {
            outputBlocks[blockOffset] = page.getBlock(aggregationInputChannels[i]);
        }
        outputBlocks[blockOffset] = booleanRle(true, page.getPositionCount());
        return new Page(page.getPositionCount(), outputBlocks);
    }

    private static RunLengthEncodedBlock booleanRle(boolean value, int positionCount)
    {
        BlockBuilder valueBuilder = BOOLEAN.createBlockBuilder(null, 1);
        BOOLEAN.writeBoolean(valueBuilder, value);
        return new RunLengthEncodedBlock(valueBuilder.build(), positionCount);
    }

    private static RunLengthEncodedBlock nullRle(Type type, int positionCount)
    {
        return new RunLengthEncodedBlock(
                type.createBlockBuilder(null, 1).appendNull().build(),
                positionCount);
    }
}

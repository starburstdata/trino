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

public interface PositionsAppender
{
    void appendTo(IntArrayList positions, Block source);

    default void appendRle(RunLengthEncodedBlock rleBlock)
    {
        appendRle(rleBlock.getPositionCount(), rleBlock.getValue(), 0);
    }

    void appendRle(int positionCount, Block source, int sourcePosition);

    void appendDictionary(IntArrayList positions, DictionaryBlock source);

    Block build();

    /**
     * Returns {@code true} if the first value (at position 0) equals value in block {@code other} at position {@code otherPosition}.
     */
    boolean firstValueEquals(Block other, int otherPosition);

    /**
     * Creates new empty {@link PositionsAppender} of the same type as {@code this}.
     */
    PositionsAppender newState(BlockBuilderStatus blockBuilderStatus, int expectedPositions);

    void prepareProcessingByRow();

    void appendRow(Block source, int position);

    long getRetainedSizeInBytes();
}

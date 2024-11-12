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
package io.trino.operator.hash;

import com.google.common.annotations.VisibleForTesting;
import io.trino.operator.GroupByHash;
import io.trino.operator.UpdateMemory;
import io.trino.operator.Work;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.AbstractLongType;
import io.trino.spi.type.BigintType;
import org.openjdk.jmh.annotations.CompilerControl;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.spi.StandardErrorCode.GENERIC_INSUFFICIENT_RESOURCES;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.type.TypeUtils.NULL_HASH_CODE;
import static it.unimi.dsi.fastutil.HashCommon.arraySize;
import static it.unimi.dsi.fastutil.HashCommon.murmurHash3;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static org.openjdk.jmh.annotations.CompilerControl.Mode.DONT_INLINE;

public class VectorizedByteArraySingleArrayBigintGroupByHash_2
        implements GroupByHash
{
    private static final int INSTANCE_SIZE = instanceSize(VectorizedByteArraySingleArrayBigintGroupByHash_2.class);
    private static final int BATCH_SIZE = 1024;

    private static final float FILL_RATIO = 0.5f;
    private static final VarHandle INT_HANDLE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);
    private static final VarHandle LONG_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

    private final boolean outputRawHash;

    private int hashCapacity;
    private int maxFill;
    private int mask;

    // the hash table with values and groupIds
    private byte[] hashTable;

    // groupId for the null value
    private int nullGroupId = -1;

    // reverse index from the groupId back to the value
    private long[] valuesByGroupId;

    private int nextGroupId;

    // reserve enough memory before rehash
    private final UpdateMemory updateMemory;
    private long preallocatedMemoryInBytes;
    private long currentPageSizeInBytes;
    private final int stepSize;

    public VectorizedByteArraySingleArrayBigintGroupByHash_2(boolean outputRawHash, int expectedSize, UpdateMemory updateMemory, int stepSize)
    {
        this.stepSize = stepSize;
        checkArgument(expectedSize > 0, "expectedSize must be greater than zero");

        this.outputRawHash = outputRawHash;

        hashCapacity = arraySize(expectedSize, FILL_RATIO);

        maxFill = calculateMaxFill(hashCapacity);
        mask = hashCapacity - 1;
        hashTable = new byte[hashCapacity * 12];
        for (int i = 0; i < hashTable.length; i = i + 12) {
            INT_HANDLE.set(hashTable, i, -1);
        }
//        Arrays.fill(hashTable, (byte) -1);

        valuesByGroupId = new long[maxFill];

        // This interface is used for actively reserving memory (push model) for rehash.
        // The caller can also query memory usage on this object (pull model)
        this.updateMemory = requireNonNull(updateMemory, "updateMemory is null");
    }

    @Override
    public long getEstimatedSize()
    {
        return INSTANCE_SIZE +
                sizeOf(hashTable) +
                sizeOf(valuesByGroupId) +
                preallocatedMemoryInBytes;
    }

    @Override
    public int getGroupCount()
    {
        return nextGroupId;
    }

    @Override
    public void appendValuesTo(int groupId, PageBuilder pageBuilder)
    {
        checkArgument(groupId >= 0, "groupId is negative");
        BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(0);
        if (groupId == nullGroupId) {
            blockBuilder.appendNull();
        }
        else {
            BIGINT.writeLong(blockBuilder, valuesByGroupId[groupId]);
        }

        if (outputRawHash) {
            BlockBuilder hashBlockBuilder = pageBuilder.getBlockBuilder(1);
            if (groupId == nullGroupId) {
                BIGINT.writeLong(hashBlockBuilder, NULL_HASH_CODE);
            }
            else {
                BIGINT.writeLong(hashBlockBuilder, AbstractLongType.hash(valuesByGroupId[groupId]));
            }
        }
    }

    @Override
    public Work<?> addPage(Page page)
    {
        currentPageSizeInBytes = page.getRetainedSizeInBytes();
        Block block = page.getBlock(0);

        return new AddPageWork(block);
    }

    @Override
    public Work<int[]> getGroupIds(Page page)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getRawHash(int groupId)
    {
        return BigintType.hash(valuesByGroupId[groupId]);
    }

    @VisibleForTesting
    @Override
    public int getCapacity()
    {
        return hashCapacity;
    }

    @CompilerControl(DONT_INLINE)
    private void putIfAbsent(int position, int stepSize, Block block, int[] positionBuffer, int[] groupIdBuffer, long[] valueBuffer, int[] sequentialPut)
    {
        stepSize = Math.min(block.getPositionCount() - position, stepSize);

        calculateHashPositions(position, stepSize, block, positionBuffer);

        // preload group ids and values
        loadGroupIdsAndValues(stepSize, positionBuffer, groupIdBuffer, valueBuffer);

        int sequentialIndex = primaryLoop(position, stepSize, block, positionBuffer, groupIdBuffer, valueBuffer, sequentialPut);

        sequentialLoop(position, block, positionBuffer, sequentialPut, sequentialIndex);
    }

    @CompilerControl(DONT_INLINE)
    private void sequentialLoop(int position, Block block, int[] positionBuffer, int[] sequentialPut, int sequentialIndex)
    {
        for (int i = 0; i < sequentialIndex; i++) {
            int index = sequentialPut[i];
            long value = BIGINT.getLong(block, position + index);
            int hashPosition = positionBuffer[index];

            // look for an empty slot or a slot containing this key
            while (true) {
                int groupId = getGroupId(hashPosition);
                if (groupId == -1) {
                    addNewGroup(hashPosition, value);
                    break;
                }

                if (value == getValue(hashPosition)) {
                    break;
                }
                else {
                    // increment position and mask to handle wrap around
                    hashPosition = increment(hashPosition, hashTable.length);
                }
            }
        }
    }

    private long getValue(int hashPosition)
    {
        return (long) LONG_HANDLE.get(hashTable, hashPosition + 4);
    }

    private int getGroupId(int hashPosition)
    {
        return (int) INT_HANDLE.get(hashTable, hashPosition);
    }

    @CompilerControl(DONT_INLINE)
    private int primaryLoop(int position, int stepSize, Block block, int[] positionBuffer, int[] groupIdBuffer, long[] valueBuffer, int[] sequentialPut)
    {
        int sequentialIndex = 0;
        for (int i = 0; i < stepSize; i++) {
            int hashPosition = positionBuffer[i];
            long value = BIGINT.getLong(block, position + i);
            // look for an empty slot or a slot containing this key
            int groupId = groupIdBuffer[i];
            if (groupId == -1) {
                // this could have changed, need to check the second time
                if (getGroupId(hashPosition) == -1) {
                    // still free sloT
                    addNewGroup(hashPosition, value);
                }
                else {
                    sequentialPut[sequentialIndex++] = i;
                    positionBuffer[i] = increment(hashPosition, hashTable.length);
                }
            }
            else if (value != valueBuffer[i]) {
                sequentialPut[sequentialIndex++] = i;
                positionBuffer[i] = increment(hashPosition, hashTable.length);
            }
        }
        return sequentialIndex;
    }

    @CompilerControl(DONT_INLINE)
    private void calculateHashPositions(int position, int stepSize, Block block, int[] positionBuffer)
    {
        for (int i = 0; i < stepSize; i++) {
            if (block.isNull(position + i)) {
                throw new IllegalArgumentException();
            }
            long value = BIGINT.getLong(block, position + i);
            int hashPosition = getHashPosition(value, mask);
            positionBuffer[i] = hashPosition;
        }
    }

    @CompilerControl(DONT_INLINE)
    private void loadGroupIdsAndValues(int stepSize, int[] positionBuffer, int[] groupIdBuffer, long[] valueBuffer)
    {
        for (int i = 0; i < stepSize; i++) {
            int hashPosition = positionBuffer[i];
            groupIdBuffer[i] = getGroupId(hashPosition);
            valueBuffer[i] = getValue(hashPosition);
        }
    }

    private static int increment(int hashPosition, int length)
    {
        hashPosition = hashPosition + 12;
        if (hashPosition >= length) {
            hashPosition = 0;
        }
        return hashPosition;
    }

    private int addNewGroup(int hashPosition, long value)
    {
        // record group id in hash
        int groupId = nextGroupId++;

        valuesByGroupId[groupId] = value;
        setGroupId(hashPosition, groupId);
        setValue(hashPosition, value);

        // increase capacity, if necessary
        if (needRehash()) {
            tryRehash();
        }
        return groupId;
    }

    private void setGroupId(int hashPosition, int groupId)
    {
        INT_HANDLE.set(hashTable, hashPosition, groupId);
    }

    private void setValue(int hashPosition, long value)
    {
        LONG_HANDLE.set(hashTable, hashPosition + 4, value);
    }

    private boolean tryRehash()
    {
        long newCapacityLong = hashCapacity * 2L;
        if (newCapacityLong > Integer.MAX_VALUE) {
            throw new TrinoException(GENERIC_INSUFFICIENT_RESOURCES, "Size of hash table cannot exceed 1 billion entries");
        }
        int newCapacity = toIntExact(newCapacityLong);

        // An estimate of how much extra memory is needed before we can go ahead and expand the hash table.
        // This includes the new capacity for values, groupIds, and valuesByGroupId as well as the size of the current page
        preallocatedMemoryInBytes = newCapacity * (long) (Long.BYTES + Integer.BYTES) + ((long) calculateMaxFill(newCapacity)) * Long.BYTES + currentPageSizeInBytes;
        if (!updateMemory.update()) {
            // reserved memory but has exceeded the limit
            return false;
        }

        int newMask = newCapacity - 1;
        byte[] newHashTable = new byte[newCapacity * 12];
//        Arrays.fill(newHashTable, (byte) -1);
        for (int i = 0; i < newHashTable.length; i = i + 12) {
            INT_HANDLE.set(newHashTable, i, -1);
        }

        for (int i = 0; i < hashTable.length; i = i + 12) {
            int groupId = getGroupId(i);

            if (groupId != -1) {
                long value = getValue(i);
                int hashPosition = getHashPosition(value, newMask);

                // find an empty slot for the address
                while (((int) INT_HANDLE.get(newHashTable, hashPosition)) != -1) {
                    hashPosition = increment(hashPosition, newHashTable.length);
                }

                // record the mapping
                INT_HANDLE.set(newHashTable, hashPosition, groupId);
                LONG_HANDLE.set(newHashTable, hashPosition + 4, value);
            }
        }

        mask = newMask;
        hashCapacity = newCapacity;
        maxFill = calculateMaxFill(hashCapacity);
        hashTable = newHashTable;

        this.valuesByGroupId = Arrays.copyOf(valuesByGroupId, maxFill);

        preallocatedMemoryInBytes = 0;
        // release temporary memory reservation
        updateMemory.update();
        return true;
    }

    private boolean needRehash()
    {
        return nextGroupId >= maxFill;
    }

    private static int getHashPosition(long rawHash, int mask)
    {
        return ((int) (murmurHash3(rawHash) & mask)) * 12;
    }

    private static int calculateMaxFill(int hashSize)
    {
        checkArgument(hashSize > 0, "hashSize must be greater than 0");
        int maxFill = (int) Math.ceil(hashSize * FILL_RATIO);
        if (maxFill == hashSize) {
            maxFill--;
        }
        checkArgument(hashSize > maxFill, "hashSize must be larger than maxFill");
        return maxFill;
    }

    @VisibleForTesting
    class AddPageWork
            implements Work<Void>
    {
        private final Block block;

        private int lastPosition;

        public AddPageWork(Block block)
        {
            this.block = requireNonNull(block, "block is null");
        }

        @Override
        public boolean process()
        {
            int positionCount = block.getPositionCount();
            checkState(lastPosition <= positionCount, "position count out of bound");
            int remainingPositions = positionCount - lastPosition;

            int[] positionBuffer = new int[stepSize];
            int[] groupIdBuffer = new int[stepSize];
            long[] valueBuffer = new long[stepSize];
            int[] sequentialPut = new int[stepSize];
            while (remainingPositions != 0) {
                int batchSize = min(remainingPositions, BATCH_SIZE);
                if (!ensureHashTableSize(batchSize)) {
                    return false;
                }

                for (int i = lastPosition; i < lastPosition + batchSize; i = i + stepSize) {
                    putIfAbsent(i, stepSize, block, positionBuffer, groupIdBuffer, valueBuffer, sequentialPut);
                }

                lastPosition += batchSize;
                remainingPositions -= batchSize;
            }
            verify(lastPosition == positionCount);
            return true;
        }

        @Override
        public Void getResult()
        {
            throw new UnsupportedOperationException();
        }
    }

    private boolean ensureHashTableSize(int batchSize)
    {
        int positionCountUntilRehash = maxFill - nextGroupId;
        while (positionCountUntilRehash < batchSize) {
            if (!tryRehash()) {
                return false;
            }
            positionCountUntilRehash = maxFill - nextGroupId;
        }
        return true;
    }
}

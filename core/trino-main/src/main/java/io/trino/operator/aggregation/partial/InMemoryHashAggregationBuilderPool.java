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

import io.trino.operator.aggregation.builder.InMemoryHashAggregationBuilder;

import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayDeque;
import java.util.Optional;
import java.util.Queue;

public class InMemoryHashAggregationBuilderPool
{
    /**
     * Maximum number of free {@link InMemoryHashAggregationBuilder}s.
     * In normal conditions, in the steady state,
     * the number of free {@link InMemoryHashAggregationBuilder}s is going to be close to 0.
     * There is a possible case though, where initially big number of concurrent drivers, say 128,
     * drops to a small number e.g. 32 in a steady state. This could cause a lot of memory
     * to be retained by the unused buffers.
     * To defend against that, {@link #maxFree} limits the number of free buffers,
     * thus limiting unused memory.
     */
    private final int maxFree;
    @GuardedBy("this")
    private final Queue<InMemoryHashAggregationBuilder> free = new ArrayDeque<>();

    public InMemoryHashAggregationBuilderPool(int maxFree)
    {
        this.maxFree = maxFree;
    }

    public synchronized Optional<InMemoryHashAggregationBuilder> poll()
    {
        return free.isEmpty() ? Optional.empty() : Optional.of(free.poll());
    }

    /**
     * Returns true if the aggregationBuilder was successfully released to the pool,
     * otherwise it is no longer handled by the pool and should be flushed by the caller.
     */
    public boolean tryRelease(InMemoryHashAggregationBuilder aggregationBuilder)
    {
        synchronized (this) {
            boolean shouldRetain = free.size() < maxFree;
            if (shouldRetain) {
                free.add(aggregationBuilder);
            }
            return shouldRetain;
        }
    }
}

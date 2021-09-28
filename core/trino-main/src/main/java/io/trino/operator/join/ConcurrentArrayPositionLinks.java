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
package io.trino.operator.join;

import com.google.common.base.Preconditions;
import io.airlift.slice.Slices;
import io.airlift.slice.XxHash64;
import io.trino.spi.Page;
import org.openjdk.jol.info.ClassLayout;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;

import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public final class ConcurrentArrayPositionLinks
        implements PositionLinks
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ConcurrentArrayPositionLinks.class).instanceSize();

    public interface ConcurrentFactoryBuilder
            extends PositionLinks.FactoryBuilder
    {

        void linkAndUpdate(IntAtomicArray key, long pos, int currentKey, int realPosition);
    }

    public static class FactoryBuilder
            implements ConcurrentFactoryBuilder
    {
        private static final VarHandle AA
                = MethodHandles.arrayElementVarHandle(int[].class);
        private final int[] positionLinks;
        private final LongAdder size = new LongAdder();

        private FactoryBuilder(int size)
        {
            positionLinks = new int[size];
            Arrays.fill(positionLinks, -1);
        }

        @Override
        public int link(int left, int right)
        {
            size.increment();
            Preconditions.checkArgument(AA.compareAndSet(positionLinks, left, -1, right));
            return left;
        }

        @Override
        public Factory build()
        {
            return new Factory()
            {
                @Override
                public PositionLinks create(List<JoinFilterFunction> searchFunctions)
                {
                    return new ConcurrentArrayPositionLinks(positionLinks);
                }

                @Override
                public long checksum()
                {
                    return XxHash64.hash(Slices.wrappedIntArray(positionLinks));
                }
            };
        }

        @Override
        public int size()
        {
            return size.intValue();
        }

        @Override
        public void linkAndUpdate(IntAtomicArray key, long pos, int currentKey, int realPosition)
        {
            while (!key.compareAndSet(pos, currentKey, realPosition)) {
                currentKey = key.get(pos);
            }
            // value was successfully updated
            // link the new key position to the current key position
            link(realPosition, currentKey);
        }
    }

    private final int[] positionLinks;

    private ConcurrentArrayPositionLinks(int[] positionLinks)
    {
        this.positionLinks = requireNonNull(positionLinks, "positionLinks is null");
    }

    public static FactoryBuilder builder(int size)
    {
        return new FactoryBuilder(size);
    }

    @Override
    public int start(int position, int probePosition, Page allProbeChannelsPage)
    {
        return position;
    }

    @Override
    public int next(int position, int probePosition, Page allProbeChannelsPage)
    {
        return positionLinks[position];
    }

    @Override
    public long getSizeInBytes()
    {
        return INSTANCE_SIZE + sizeOf(positionLinks);
    }
}

package io.trino.operator.join;

import com.google.common.primitives.Ints;
import io.airlift.slice.SizeOf;

import java.util.concurrent.atomic.AtomicIntegerArray;

public class SmallIntAtomicArray
        implements IntAtomicArray
{
    private final AtomicIntegerArray a;

    public SmallIntAtomicArray(final int capacity)
    {
        if (capacity < 0) {
            throw new IllegalArgumentException("Initial capacity (" + capacity + ") is negative");
        }

        a = new AtomicIntegerArray(capacity);
    }

    @Override
    public void fill(final int value)
    {
        for (int i = 0; i < a.length(); i++) {
            a.setPlain(i, value);
        }
    }

    @Override
    public long sizeInBytes()
    {
        return SizeOf.sizeOfIntArray(a.length());
    }

    @Override
    public int get(long index)
    {
        return a.get(Ints.checkedCast(index));
    }

    @Override
    public boolean compareAndSet(long index, int expected, int newValue)
    {
        return a.compareAndSet(Ints.checkedCast(index), expected, newValue);
    }

    @Override
    public int getPlain(long index)
    {
        return a.getPlain(Ints.checkedCast(index));
    }
}

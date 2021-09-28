package io.trino.operator.join;

import io.airlift.slice.SizeOf;
import it.unimi.dsi.fastutil.BigArrays;
import it.unimi.dsi.fastutil.ints.IntBigArrays;

import java.util.concurrent.atomic.AtomicIntegerArray;

import static it.unimi.dsi.fastutil.BigArrays.displacement;
import static it.unimi.dsi.fastutil.BigArrays.segment;

public class BigIntAtomicArray
        implements IntAtomicArray
{
    private final AtomicIntegerArray[] a;

    public BigIntAtomicArray(final long capacity)
    {
        if (capacity < 0) {
            throw new IllegalArgumentException("Initial capacity (" + capacity + ") is negative");
        }

        a = IntBigArrays.newBigAtomicArray(capacity);
    }

    @Override
    public void fill(final int value)
    {
        for (int i = a.length; i-- != 0; ) {
            AtomicIntegerArray array = a[i];
            for (int j = 0; j < array.length(); j++) {
                array.setPlain(j, value);
            }
        }
    }

    @Override
    public long sizeInBytes()
    {
        long size = 0;
        for (AtomicIntegerArray array : a) {
            size = SizeOf.sizeOfIntArray(array.length());
        }
        return size;
    }

    @Override
    public int get(long index)
    {
        return BigArrays.get(a, index);
    }

    @Override
    public boolean compareAndSet(long index, int expected, int newValue)
    {
        return BigArrays.compareAndSet(a, index, expected, newValue);
    }

    @Override
    public int getPlain(long index)
    {
        return a[segment(index)].getPlain(displacement(index));
    }
}

package io.trino.operator.join;

public interface IntAtomicArray
{
    void fill(int value);

    long sizeInBytes();

    int get(long index);

    boolean compareAndSet(long index, int expected, int newValue);

    int getPlain(long index);
}

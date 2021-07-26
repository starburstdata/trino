package io.trino.benchmark;

public class MetaSum
{
    final SumInterface sumInterface;

    public MetaSum(SumInterface sumInterface)
    {
        this.sumInterface = sumInterface;
    }

    public int sum(int a, int b)
    {
        return sumInterface.sum(a, b);
    }
}

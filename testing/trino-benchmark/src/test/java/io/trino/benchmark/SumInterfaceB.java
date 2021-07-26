package io.trino.benchmark;

public class SumInterfaceB
        implements SumInterface
{
    @Override
    public int sum(int a, int b)
    {
        return IntSum.sum(a, b) + 2;
    }
}

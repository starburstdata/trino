package io.trino.benchmark;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.RunnerException;

import java.lang.invoke.CallSite;
import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.concurrent.TimeUnit;

import static io.trino.jmh.Benchmarks.benchmark;

/**
 * This benchmark a case when there is almost like a cross join query
 * but with very selective inequality join condition. In other words
 * for each probe position there are lots of matching build positions
 * which are filtered out by filtering function.
 */
@State(Scope.Benchmark)
@Fork(2)
@Warmup(iterations = 10, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 2, timeUnit = TimeUnit.SECONDS)
public class BenchmarkFoo
{

    public int first;
    public int second;
    public final MethodHandle mhh;
    private final SumInterface lambdaMetafactoryFunction;
    public MetaSum sumInterface;

    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @BenchmarkMode(Mode.AverageTime)
    public int directMethodCall()
    {
        int result = 0;
        for (int i = 0; i < 10_000; i++) {
            result += IntSum.sum(first, result);
        }
        return result;
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @BenchmarkMode(Mode.AverageTime)
    public int finalMethodHandle()
            throws Throwable
    {
        int result = 0;
        for (int i = 0; i < 10_000; ++i) {
            result += (int) mhh.invokeExact(first, result);
        }
        return result;
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @BenchmarkMode(Mode.AverageTime)
    public int lambdaMetafactory()
    {
        int result = 0;
        for (int i = 0; i < 10_000; ++i) {
            result += lambdaMetafactoryFunction.sum(first, result);
        }
        return result;
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @BenchmarkMode(Mode.AverageTime)
    public int interfaceSum()
            throws Throwable
    {
        //return sumInterface.sum(first, second);
        int result = 0;
        for (int i = 0; i < 10_000; ++i) {
            result += sumInterface.sum(first, result);
        }
        return result;
    }

    public BenchmarkFoo()
    {
        MethodHandles.Lookup lookup = MethodHandles.lookup();
        try {
            mhh = lookup.findStatic(IntSum.class, "sum", MethodType.methodType(int.class, int.class, int.class));
        }
        catch (NoSuchMethodException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }

        // LambdaMetafactory setup
        try {
            CallSite site = LambdaMetafactory.metafactory(
                    lookup,
                    "sum",
                    MethodType.methodType(SumInterface.class), //signature of lambda factory
                    MethodType.methodType(int.class, int.class, int.class), // signature of method SumInterface#sum
                    mhh,
                    mhh.type());
            lambdaMetafactoryFunction = (SumInterface) site.getTarget().invokeExact();
        }
        catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }

    @Setup
    public void setup()
            throws Throwable
    {
        first = 9857893;
        second = 893274;
        /*sumInterface = new MetaSum(new SumInterfaceD());
        interfaceSum();
        sumInterface = new MetaSum(new SumInterfaceC());
        interfaceSum();
        sumInterface = new MetaSum(new SumInterfaceB());
        interfaceSum();
        sumInterface = new MetaSum(new SumInterfaceA());*/


        first = new MetaSum(new SumInterfaceB()).sum(first, 0);
        first = new MetaSum(new SumInterfaceC()).sum(first, 0);
        first = new MetaSum(new SumInterfaceD()).sum(first, 0);
        first = new MetaSum(new SumInterfaceA()).sum(first, 0);
        sumInterface = new MetaSum(new SumInterfaceA());
        interfaceSum();
        sumInterface = new MetaSum(new SumInterfaceA());
        interfaceSum();
        sumInterface = new MetaSum(new SumInterfaceA());
        interfaceSum();
        sumInterface = new MetaSum(new SumInterfaceA());
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(BenchmarkFoo.class)
                //.withOptions(v -> v.jvmArgsAppend("-XX:+UnlockDiagnosticVMOptions", "-XX:+PrintAssembly"))
                //.withOptions(v -> v.addProfiler("perfasm"))
                .run();
    }
}

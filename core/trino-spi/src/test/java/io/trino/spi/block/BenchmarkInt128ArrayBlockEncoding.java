package io.trino.spi.block;

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SliceOutput;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.Random;

import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.spi.type.Decimals.MAX_PRECISION;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.openjdk.jmh.annotations.Scope.Thread;

@OutputTimeUnit(NANOSECONDS)
@Fork(1)
@Warmup(iterations = 10, time = 1)
@Measurement(iterations = 10, time = 1)
@BenchmarkMode(AverageTime)
public class BenchmarkInt128ArrayBlockEncoding
{
    private static final int POSITION_COUNT = 8128;

    @Benchmark
    @OperationsPerInvocation(POSITION_COUNT)
    public Object writeBlock(BenchmarkData data)
    {
        Int128ArrayBlockEncoding encoding = new Int128ArrayBlockEncoding();
        SliceOutput sliceOutput = new DynamicSliceOutput(POSITION_COUNT * 16);
        encoding.writeBlock(null, sliceOutput, data.getBlock());
        return sliceOutput;
    }

    @State(Thread)
    public static class BenchmarkData
    {
        private static final Random RANDOM = new Random(321434);

        @Param({"SHORT", "LONG"})
        private String decimalSize = "LONG";

        @Param({"0", "0.2", "0.5", "0.8", "1"})
        private double nullRate = 0.2;

        private Block block;
        private DecimalType type;

        @Setup
        public void setup()
        {
            type = DecimalType.createDecimalType(MAX_PRECISION, 0);
            BlockBuilder blockBuilder = new Int128ArrayBlockBuilder(null, POSITION_COUNT);
            int nullCount = (int) (nullRate * POSITION_COUNT);
            for (int i = 0; i < POSITION_COUNT; i++) {
                if (RANDOM.nextInt(POSITION_COUNT) >= nullCount) {
                    Int128 value = "LONG".equals(decimalSize) ? Int128.valueOf(RANDOM.nextLong(), RANDOM.nextLong()) : Int128.valueOf(0, RANDOM.nextLong());
                    type.writeObject(blockBuilder, value);
                }
                else {
                    blockBuilder.appendNull();
                }
            }
            block = blockBuilder.build();
        }

        public Block getBlock()
        {
            return block;
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        benchmark(BenchmarkInt128ArrayBlockEncoding.class).run();
    }
}

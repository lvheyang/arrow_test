package io.lvheyang;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgs = {"-Xms2G", "-Xmx2G", "-XX:+UseSuperWord"})
@Warmup(iterations = 3)
@Measurement(iterations = 5)
public class AddTotal2Bench {

    @Param({"4000000"})
    private int N;

    @Benchmark
    public void loopFor(Blackhole bh) {

        int[] vector = new int[N];
        for (int i = 0; i < N; i++) {
            int a = 1;
            int b = 2;
            int c = 3;
            vector[i] = a + b + c;
        }
    }

}

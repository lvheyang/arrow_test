package io.lvheyang;

import org.apache.arrow.gandiva.expression.TreeBuilder;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.joda.time.DateTime;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgs = {"-Xms2G", "-Xmx2G", "-XX:+UseSuperWord"})
@Warmup(iterations = 3)
@Measurement(iterations = 5)
public class AddTotalBenchmark {

    @Param({"4000000"})
    private int N;

    private VectorSchemaRoot vsr;
    private IntVector vector;
    private RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);


    public VectorSchemaRoot createVSR() {
        BitVector bitVector = new BitVector("host", allocator);
        VarCharVector varCharVector = new VarCharVector("_raw", allocator);
        DateMilliVector timeVector = new DateMilliVector("_time", allocator);
        IntVector vectorA = new IntVector("a", allocator);
        IntVector vectorB = new IntVector("b", allocator);
        IntVector vectorC = new IntVector("c", allocator);

        bitVector.allocateNew();
        varCharVector.allocateNew();
        timeVector.allocateNew();
        vectorA.allocateNew();
        vectorB.allocateNew();
        vectorC.allocateNew();

        long start = 1577808000000L;
        for (int i = 0; i < N; i++) {
            DateTime dt = new DateTime(start + i);
            int f1 = i % 2 == 0 ? 0 : 1;

            timeVector.setSafe(i, dt.getMillis());
            bitVector.setSafe(i, f1);
            varCharVector.setSafe(i, (String.format("<Line:%s>|[%s]|message:%s|f1:%s", i, dt, "This is test log", f1)).getBytes(StandardCharsets.UTF_8));
            vectorA.setSafe(i, 1);
            vectorB.setSafe(i, 2);
            vectorC.setSafe(i, 3);
        }
        bitVector.setValueCount(N);
        varCharVector.setValueCount(N);
        timeVector.setValueCount(N);
        vectorA.setValueCount(N);
        vectorB.setValueCount(N);
        vectorC.setValueCount(N);

        List<Field> fields = Arrays.asList(
                timeVector.getField(),
                varCharVector.getField(),
                bitVector.getField(),
                vectorA.getField(),
                vectorB.getField(),
                vectorC.getField());
        List<FieldVector> vectors = Arrays.asList(
                timeVector,
                varCharVector,
                bitVector,
                vectorA,
                vectorB,
                vectorC);

        return new VectorSchemaRoot(fields, vectors);
    }


    @Setup
    public void setup() {
        vsr = createVSR();
        vector = new IntVector("total", allocator);
        vector.allocateNew(N);
    }

    @Benchmark
    public void loopFor(Blackhole bh) {
        IntVector vectorA = (IntVector) vsr.getVector("a");
        IntVector vectorB = (IntVector) vsr.getVector("b");
        IntVector vectorC = (IntVector) vsr.getVector("c");

        for (int i = 0; i < N; i++) {
            int a = vectorA.get(i);
            int b = vectorB.get(i);
            int c = vectorC.get(i);
            vector.set(i, a + b + c);
        }

        vector.setValueCount(vsr.getRowCount());
    }


}

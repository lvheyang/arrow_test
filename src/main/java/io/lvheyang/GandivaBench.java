package io.lvheyang;

import com.google.common.collect.Lists;
import io.netty.buffer.ArrowBuf;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.gandiva.evaluator.Projector;
import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.apache.arrow.gandiva.expression.ExpressionTree;
import org.apache.arrow.gandiva.expression.TreeBuilder;
import org.apache.arrow.gandiva.expression.TreeNode;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgs = {"-Xms2G", "-Xmx2G", "-XX:+UseSuperWord"})
@Warmup(iterations = 3)
@Measurement(iterations = 5)
public class GandivaBench {

  @Param({"4000000"})
  private int N;

  private VectorSchemaRoot vsr;
  private IntVector vector;
  private RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
  private ExpressionTree expr;
  private Projector projector = null;

  @Setup
  public void setup() {
    vsr = createVSR();
    vector = new IntVector("total", allocator);
    vector.allocateNew(N);

    TreeNode t1 = TreeBuilder.makeFunction("add",
        Arrays.asList(
            TreeBuilder.makeField(vsr.getVector("a").getField()),
            TreeBuilder.makeField(vsr.getVector("b").getField())),
        new ArrowType.Int(32, true));
    TreeNode t2 = TreeBuilder.makeFunction("add",
        Arrays.asList(
            t1,
            TreeBuilder.makeField(vsr.getVector("c").getField())),
        new ArrowType.Int(32, true));
    expr = TreeBuilder.makeExpression(t2, vector.getField());
    try {
      projector = Projector.make(vsr.getSchema(), Arrays.asList(expr));
    } catch (GandivaException e) {
      e.printStackTrace();
    }


  }

  private VectorSchemaRoot createVSR() {
    IntVector vectorA = new IntVector("a", allocator);
    IntVector vectorB = new IntVector("b", allocator);
    IntVector vectorC = new IntVector("c", allocator);
    IntVector vector = new IntVector("total", allocator);
    vectorA.allocateNew(N);
    vectorB.allocateNew(N);
    vectorC.allocateNew(N);
    vector.allocateNew(N);

    for (int i = 0; i < N; i++) {

      vectorA.setSafe(i, 1);
      vectorB.setSafe(i, 2);
      vectorC.setSafe(i, 3);
    }

    vectorA.setValueCount(N);
    vectorB.setValueCount(N);
    vectorC.setValueCount(N);

    List<Field> fields = Arrays.asList(
        vectorA.getField(),
        vectorB.getField(),
        vectorC.getField());
    List<FieldVector> vectors = Arrays.asList(
        vectorA,
        vectorB,
        vectorC);

    return new VectorSchemaRoot(fields, vectors);
  }


  @Benchmark
  public void loopFor(Blackhole bh) {

    List<ArrowBuf> buffers = Lists.newArrayList();
    for (FieldVector v : vsr.getFieldVectors()) {
      buffers.addAll(v.getFieldBuffers());
    }

    try {
      projector.evaluate(N, buffers, Arrays.asList(vector));
    } catch (GandivaException e) {
      e.printStackTrace();
    }

  }
}

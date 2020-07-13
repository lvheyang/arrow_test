package io.lvheyang;

import com.google.common.collect.Lists;
import io.netty.buffer.ArrowBuf;
import java.beans.Expression;
import java.util.Arrays;
import java.util.List;
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

public class GandivaMain {

  private final static int N = 4_000_000;

  public static void main(String[] args) {
    RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);

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

    VectorSchemaRoot vsr = new VectorSchemaRoot(fields, vectors);

    TreeNode t1 = TreeBuilder.makeFunction("add",
        Arrays.asList(
            TreeBuilder.makeField(vectorA.getField()),
            TreeBuilder.makeField(vectorB.getField())),
        new ArrowType.Int(32, true));
    TreeNode t2 = TreeBuilder.makeFunction("add",
        Arrays.asList(
            t1,
            TreeBuilder.makeField(vectorC.getField())),
        new ArrowType.Int(32, true));
    ExpressionTree expr = TreeBuilder.makeExpression(t2, vector.getField());

    try {
      Projector projector = Projector.make(vsr.getSchema(), Arrays.asList(expr));
      List<ArrowBuf> buffers = Lists.newArrayList();
      for (FieldVector v : vsr.getFieldVectors()) {
        buffers.addAll(v.getFieldBuffers());
      }
      long start = System.currentTimeMillis();

      projector.evaluate(N, buffers, Arrays.asList(vector));

      System.out
          .println("(System.currentTimeMillis() - start) = " + (System.currentTimeMillis() - start));

      System.out.println("vsr.contentToTSVString() = " + vector.toString());
    } catch (GandivaException e) {
      e.printStackTrace();
    }

  }
}

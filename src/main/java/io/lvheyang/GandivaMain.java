package io.lvheyang;

import org.apache.arrow.gandiva.evaluator.Projector;
import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.apache.arrow.gandiva.expression.ExpressionTree;
import org.apache.arrow.gandiva.expression.TreeBuilder;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.joda.time.DateTime;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

public class GandivaMain {

    private final static int N = 100;

    public static void main(String[] args) {
        RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);

        IntVector vectorA = new IntVector("a", allocator);
        IntVector vectorB = new IntVector("b", allocator);
        IntVector vectorC = new IntVector("c", allocator);
        IntVector vector = new IntVector("total", allocator);
        vectorA.allocateNew();
        vectorB.allocateNew();
        vectorC.allocateNew();
        vector.allocateNew();

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

        ExpressionTree expr = TreeBuilder.makeExpression(
                "add",
                Arrays.asList(vectorA.getField(), vectorB.getField(), vectorC.getField()),
                vector.getField()
        );


        try {
            Projector projector = Projector.make(vsr.getSchema(), Arrays.asList(expr));
            projector.evaluate(N, Arrays.asList(vectorA.getDataBuffer(), vectorB.getDataBuffer(), vectorC.getDataBuffer()), Arrays.asList(vector));
        } catch (GandivaException e) {
            e.printStackTrace();
        }

    }
}

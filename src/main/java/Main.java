import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.types.pojo.Field;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

public class Main {

    public static void main(String[] args) {
        RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);

        NullableBitHolder nullHolder = new NullableBitHolder();
        BitVector bitVector = new BitVector("boolean", allocator);
        VarCharVector varCharVector = new VarCharVector("varchar", allocator);
        bitVector.allocateNew();
        varCharVector.allocateNew();
        for (int i = 0; i < 10; i++) {
            bitVector.setSafe(i, i % 2 == 0 ? 0 : 1);
            varCharVector.setSafe(i, ("test" + i).getBytes(StandardCharsets.UTF_8));
        }
        bitVector.setValueCount(10);
        varCharVector.setValueCount(10);

        List<Field> fields = Arrays.asList(bitVector.getField(), varCharVector.getField());
        List<FieldVector> vectors = Arrays.asList(bitVector, varCharVector);
        VectorSchemaRoot vectorSchemaRoot = new VectorSchemaRoot(fields, vectors);
        vectorSchemaRoot.syncSchema();
        System.out.println("vectorSchemaRoot.contentToTSVString() = " + vectorSchemaRoot.contentToTSVString());
    }

    private void vectorizePerson(int index, Person person, VectorSchemaRoot schemaRoot) {
        // Using setSafe: it increases the buffer capacity if needed
        ((VarCharVector) schemaRoot.getVector("firstName")).setSafe(index, person.getFirstName().getBytes());
        ((VarCharVector) schemaRoot.getVector("lastName")).setSafe(index, person.getLastName().getBytes());
        ((UInt4Vector) schemaRoot.getVector("age")).setSafe(index, person.getAge());

        List<FieldVector> childrenFromFields = schemaRoot.getVector("address").getChildrenFromFields();

        Address address = person.getAddress();
        ((VarCharVector) childrenFromFields.get(0)).setSafe(index, address.getStreet().getBytes());
        ((UInt4Vector) childrenFromFields.get(1)).setSafe(index, address.getStreetNumber());
        ((VarCharVector) childrenFromFields.get(2)).setSafe(index, address.getCity().getBytes());
        ((UInt4Vector) childrenFromFields.get(3)).setSafe(index, address.getPostalCode());
    }


    void writeToArrowFile(Person[] people) throws IOException {
        new ChunkedWriter<>(ChunkedWriter.CHUNK_SIZE, this::vectorizePerson).write(new File("people.arrow"), people);
    }

}

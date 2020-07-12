import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class ChunkedWriter<T extends Vectorizer> {
    public static final int CHUNK_SIZE = 1000;
    private final int chunkSize;
    private final T vectorizer;

    public ChunkedWriter(int chunkSize, T vectorizer) {
        this.chunkSize = chunkSize;
        this.vectorizer = vectorizer;
    }

    public void write(File file, Person[] values) throws IOException {
        DictionaryProvider.MapDictionaryProvider dictProvider = new DictionaryProvider.MapDictionaryProvider();

        try (RootAllocator allocator = new RootAllocator();
             VectorSchemaRoot schemaRoot = VectorSchemaRoot.create(personSchema(), allocator);
             FileOutputStream fd = new FileOutputStream(file);
             ArrowFileWriter fileWriter = new ArrowFileWriter(schemaRoot, dictProvider, fd.getChannel())) {
            fileWriter.start();

            int index = 0;
            while (index < values.length) {
                schemaRoot.allocateNew();
                int chunkIndex = 0;
                while (chunkIndex < chunkSize && index + chunkIndex < values.length) {
                    vectorizer.vectorize(chunkIndex, values[index + chunkIndex], schemaRoot);
                    chunkIndex++;
                }
                schemaRoot.setRowCount(chunkIndex);
                fileWriter.writeBatch();

                index += chunkIndex;
                schemaRoot.clear();
            }
            fileWriter.end();
        }
    }


    private Schema personSchema() {
        return null;
    }
}

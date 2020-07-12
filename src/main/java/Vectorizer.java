import org.apache.arrow.vector.VectorSchemaRoot;

public interface Vectorizer {

    void vectorize(int chunkIndex, Person value, VectorSchemaRoot schemaRoot);
}

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.Arrays;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

public class FlightDataSender {
    public static void main(String[] args) {
        // Create a Flight client
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             FlightClient client = FlightClient.builder().allocator(allocator).location(Location.forGrpcInsecure("localhost", 3000)).build()) {

            // Define table schema
            Schema schema = new Schema(
                Arrays.asList(
                    Field.nullable("id", new ArrowType.Int(32, true)),
                    Field.nullable("name", new ArrowType.Utf8()),
                    Field.nullable("age", new ArrowType.Int(32, true))
                )
            );

            // Create a FlightData instance with the table data
            try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
                root.allocateNew(); // Allocate memory for vectors

                // Populate table data
                for (int i = 0; i < 5; i++) {
                    // Set row count to indicate a new row is being added
                    root.setRowCount(i + 1);

                    // Set values for "id" column
                    IntVector idVector = (IntVector) root.getVector("id");
                    idVector.setSafe(i, i);

                    // Set values for "name" column
                    VarCharVector nameVector = (VarCharVector) root.getVector("name");
                    byte[] nameBytes = ("John" + i).getBytes(); // Convert string to bytes
                    nameVector.setSafe(i, nameBytes);

                    // Set values for "age" column
                    IntVector ageVector = (IntVector) root.getVector("age");
                    ageVector.setSafe(i, 30 + i);
                }

                // // Create a FlightDescriptor
                // FlightDescriptor descriptor = FlightDescriptor.path("example");

                // // Start uploading data to the server
                // try (FlightClient.ClientStreamListener listener = client.startPut(descriptor, root, null)) {
                //     // Wait for the stream to be ready
                //     listener.getResult();
                // }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

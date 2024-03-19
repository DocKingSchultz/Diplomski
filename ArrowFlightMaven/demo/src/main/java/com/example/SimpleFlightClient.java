package com.example;

import org.apache.arrow.flight.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

/**
 * FlightServerExample is a class that demonstrates how to create a flight
 * server using Apache Arrow Flight.
 */

// Suppress Checkstyle warnings for the entire class
@SuppressWarnings("checkstyle:*")
public class SimpleFlightClient {
    public static void main(String[] args) {
        try {
            // Set up connection parameters
            String host = "localhost";
            int port = 3000;
            Location location = Location.forGrpcInsecure(host, port);
            try (BufferAllocator allocator = new RootAllocator();
                    FlightClient client = FlightClient.builder(allocator, location).build()) {
                        try(FlightStream flightStream = client.getStream(new Ticket(new byte[]{}))) {
                            // ...
                            CallOptions cop;
                            client.handshake(CallOption.De);

                        }

                // ... Consume operations exposed by Flight server
            } catch (Exception e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();

    }
}
}

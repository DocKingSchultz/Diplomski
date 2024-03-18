package com.example;
import org.apache.arrow.flight.*;
import org.apache.arrow.memory.RootAllocator;


/**
 * FlightServerExample is a class that demonstrates how to create a flight server using Apache Arrow Flight.
 */

// Suppress Checkstyle warnings for the entire class
@SuppressWarnings("checkstyle:*")
public class FlightServerExample {
    public static void main(String[] args) {
        try {
            // Set up connection parameters
            String host = "localhost";
            int port = 47470;
            Location location = Location.forGrpcTls(host, port);

            // Create a Flight client
            FlightClient client = FlightGrpcUtils.createFlightClient(location);

            // Construct criteria for getting flight information
            FlightCriteria criteria = FlightCriteria.ALL;

            // Get information about available flights
            FlightInfo flightInfo = client.getInfo(criteria);
            System.out.println("Available flights:");
            for (FlightInfo.FlightDescriptor descriptor : flightInfo.getDescriptors()) {
                System.out.println(descriptor.getFlightDescriptor().getType());
            }

            // Request a flight
            FlightProducer producer = client.getStream(new Ticket(new byte[0]));
            FlightStream flightStream = producer.getStream(new RootAllocator(Long.MAX_VALUE));

            // Process the flight data
            while (flightStream.next()) {
                // Your code to process flight data goes here
                // Example: System.out.println(flightStream.getRoot().schema().getFields());
            }

            // Close the Flight client
            client.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

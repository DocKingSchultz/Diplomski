const express = require('express');
const grpc = require('@grpc/grpc-js');
const { FlightService } = require('@apache-arrow/flight');

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware to parse JSON requests
app.use(express.json());

// Route handler for HTTP/JSON POST requests to /upload
app.post('/upload', (req, res) => {
  // Handle HTTP/JSON data upload here
  console.log('Received HTTP/JSON data:', req.body.tableData);
  res.send('HTTP/JSON Data received successfully');
});

// gRPC server setup
const grpcServer = new grpc.Server();

// Implement gRPC method for Put
grpcServer.addService(FlightService, {
  doPut: (call, callback) => {
    // Handle gRPC/Arrow data upload here
    call.on('data', (data) => {
      console.log('Received gRPC/Arrow data:', data);
    });

    call.on('end', () => {
      console.log('gRPC/Arrow data stream ended');
      callback(null, {}); // Send empty response
    });
  }
});

// Start gRPC server
grpcServer.bindAsync('0.0.0.0:50051', grpc.ServerCredentials.createInsecure(), () => {
  console.log('gRPC/Arrow Server is running on port 50051');
});

// Route handler for the root path
app.get('/', (req, res) => {
  res.send('Hello, World!');
});

// Start HTTP/JSON server
app.listen(PORT, () => {
  console.log(`HTTP/JSON Server is running on http://localhost:${PORT}`);
});

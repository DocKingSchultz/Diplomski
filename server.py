import pyarrow.flight as flight
import pyarrow as pa
import pandas as pd
import io
from http.server import BaseHTTPRequestHandler, HTTPServer
import json

class MyFlightServer(flight.FlightServerBase):
    data_table = []
    def do_put(self, context, descriptor, reader, writer):
        # Example: Read the stream and collect data into a Pandas DataFrame
        table = reader.read_all()
        pandas_df = table.to_pandas()
        #print(table)
        
    def do_exchange(self, context, descriptor, reader, writer):
        try:
            # Example: Read the stream and collect data into a Pandas DataFrame
            schema = reader.schema
            # Initialize the writer with the schema
            table = reader.read_all()
            self.data_table.append(table.to_pandas())
            arrow_table_to_send = pd.concat(self.data_table)
            writer.begin(schema)
            writer.write_table(pa.Table.from_pandas(arrow_table_to_send))
            writer.close()
        except Exception as e:
            print("Error occurred during data exchange:", e)

class HTTPRequestHandler(BaseHTTPRequestHandler):
    data_table = []
    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        raw_bytes = self.rfile.read(content_length)
        
        # Append raw bytes to data table
        self.data_table.append(raw_bytes)
        # Send success response
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        response_data = {'status': 'success', 'message': 'Data received successfully'}
        self.wfile.write(json.dumps(response_data).encode())
        #print(self.data_table)        
    def do_GET(self):
        # Handle GET requests
        if self.path == '/get_table':
            # If the client requests data, concatenate the raw bytes
            concatenated_data = b"".join(self.data_table)
            self.send_response(200)
            self.send_header('Content-type', 'text/csv')
            self.end_headers()
            
            # Send concatenated data to the client
            self.wfile.write(concatenated_data)
        else:
            # Handle other GET requests
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"404 - Not Found")

def start_flight_server():
    server = MyFlightServer(('0.0.0.0', 3000))  # Listen on all interfaces
    print("Starting Flight server on port 3000")
    server.serve()

def start_http_server():
    server_address = ('', 8080)  # Listen on all interfaces, port 8080
    httpd = HTTPServer(server_address, HTTPRequestHandler)
    print("Starting HTTP server on port 8080")
    httpd.serve_forever()

if __name__ == "__main__":
    import threading
    # Start the Flight server in a separate thread
    flight_server_thread = threading.Thread(target=start_flight_server)
    flight_server_thread.start()
    
    # Start the HTTP server
    start_http_server()


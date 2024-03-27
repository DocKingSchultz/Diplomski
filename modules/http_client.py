import requests
import pandas as pd
import io

SERVER_URL = 'http://localhost:8080'  # Replace with your server URL

# Function to read data from a file (optional)
def read_data_from_file(file_path):
    with open(file_path, 'r') as file:
        return file.read()

# Example data (replace this with your actual data)
data_to_send = {
    'tableName': 'example_table',
    # You can replace the content with your actual large data table
    'tableData': read_data_from_file('resources/data_csv_100.csv')  # Read data from a file (optional)
}

# Benchmarking: Start the timer
import time
start_time = time.time()

# Make a POST request to the server with the data
try:
    response = requests.post(f'{SERVER_URL}/upload', json=data_to_send)
    # Benchmarking: Stop the timer


    if response.status_code == 200:
        print('Data sent successfully:', response.json())
        response = requests.get(f'{SERVER_URL}/get_table')
        
        end_time = time.time()
        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Decode the response content as CSV data
            csv_data = response.content.decode('utf-8')
            
            # Parse CSV data into a DataFrame
           # df = pd.read_csv(io.StringIO(csv_data))
            
            # Now you can work with the DataFrame
            #print(df.head())
        else:
            # Print an error message if the request was not successful
            print("Error:", response.status_code)

        elapsed_time = (end_time - start_time) * 1000  # Convert to milliseconds
        print('Elapsed time:', f'{elapsed_time:.2f}', 'milliseconds')
        
    else:
        print('Failed to send data. Status code:', response.status_code)
except requests.RequestException as e:
    print('Error sending data:', e)
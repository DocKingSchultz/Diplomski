import pyarrow as pa
import pandas as pd
import configparser

class ParseAndCreateTable:
    def __init__(self, properties_file):
        self.properties_file = properties_file
        self.chunk_size, self.data_file_path = self.read_properties()
        self.data_table_chunked = []

    def read_properties(self):
        config = configparser.ConfigParser()
        config.read(self.properties_file)
        chunk_size = int(config['SETTINGS']['chunk_size'])
        data_file_path = config['SETTINGS']['data_file_path']
        return chunk_size, data_file_path

    def initialize_table(self):
        with open(self.data_file_path, 'r') as file:
            lines = file.readlines()

        # Extract column names from the first line
        column_names = lines[0].strip().split(',')

        # Parse data rows
        data_rows = []
        for line in lines[1:]:
            values = line.strip().split(',')
            data_rows.append(dict(zip(column_names, values)))

        # Chunk the data rows
        chunked_data = [data_rows[i:i+self.chunk_size] for i in range(0, len(data_rows), self.chunk_size)]

        # Convert each chunk to a DataFrame and then to an Arrow Table
        for chunk in chunked_data:
            df = pd.DataFrame(chunk)
            table = pa.Table.from_pandas(df)
            self.data_table_chunked.append(table)

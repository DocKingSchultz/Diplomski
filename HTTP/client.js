const axios = require('axios');
const fs = require('fs'); // For reading data from a file (optional)

const SERVER_URL = 'http://localhost:3000'; // Replace with your server URL

// Function to read data from a file (optional)
function readDataFromFile(filePath) {
  return fs.readFileSync(filePath, 'utf8');
}

// Example data (replace this with your actual data)
const dataToSend = {
  tableName: 'example_table',
  // You can replace the content with your actual large data table
  tableData: readDataFromFile('data-table.txt') // Read data from a file (optional)
};

// Benchmarking: Start the timer
const startTime = performance.now();

// Make a POST request to the server with the data
axios.post(`${SERVER_URL}/upload`, dataToSend)
  .then(response => {
    // Benchmarking: Stop the timer
    const endTime = performance.now();
    const elapsedTime = endTime - startTime;

    console.log('Data sent successfully:', response.data);
    console.log('Elapsed time:', elapsedTime.toFixed(2), 'milliseconds');
  })
  .catch(error => {
    console.error('Error sending data:', error);
  });

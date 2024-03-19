const express = require('express');

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware to parse JSON requests
app.use(express.json());

// Route handler for POST requests to /upload
app.post('/upload', (req, res) => {
  // Handle data upload here
  console.log('Received data:', req.body.tableData);
  res.send('Data received successfully');
});

// Route handler for the root path
app.get('/', (req, res) => {
  res.send('Hello, World!');
});

app.listen(PORT, () => {
  console.log(`Server is running on http://localhost:${PORT}`);
});
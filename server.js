// server.js
const express = require('express');
const multer = require('multer');
const fs = require('fs');
const path = require('path');
const axios = require('axios');
const cors = require('cors');
const { triggerAndMonitorJob } = require('./databricksJobRunner');
const { extractUsecasesFromCSV } = require('./csvUsecaseParser');
const { fetchAllLogsAndResultsForUsecases } = require('./databricksLogFetcher');

const app = express();
const port = 3000;

app.use(cors());
app.use(express.json());


let lastUploadedFile = ''; // To track uploaded CSV
let finalCatalog = '';
let finalSchema = '';
let finalLogsResult = null;

app.get('/final-result', (req, res) => {
  const { catalog, schema } = req.query;

  if (!finalLogsResult || finalCatalog !== catalog || finalSchema !== schema) {
    return res.status(202).json({ message: "Result not ready yet" });
  }

  res.status(200).json(finalLogsResult);
  // if (!finalLogsResult) return res.status(202).json({ message: "Result not ready yet" });
  // res.status(200).json(finalLogsResult);
});


// === SSE connection for live logs ===
let logClients = [];

app.get('/logs/stream', (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  logClients.push(res);

  req.on('close', () => {
    logClients = logClients.filter(client => client !== res);
  });
});

function sendLogToClients(message) {
  logClients.forEach(res => {
    res.write(`data: ${message}\n\n`);
  });
}

// === File upload setup ===
const storage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, 'uploads/'),
  filename: (req, file, cb) => cb(null, file.originalname)
});
const upload = multer({ storage });

// === POST /upload-csv ===
app.post('/upload-csv', upload.single('csvFile'), async (req, res) => {
  try {
    const filePath = path.join(__dirname, 'uploads', req.file.originalname);
    const databricksPath = `/Volumes/h_and_r/intput_file/inputs/${req.file.originalname}`;
    const fileBuffer = fs.readFileSync(filePath);
    const catalog = req.body.catalog || 'metadata';
    const schema = req.body.schema || 'metadata_schema';


    finalCatalog = catalog;
    finalSchema = schema;

    // Upload file to Databricks volume
    const uploadResponse = await axios.put(
      `https://dbc-7fdb4b6d-72d3.cloud.databricks.com/api/2.0/fs/files${databricksPath}?overwrite=true`,
      fileBuffer,
      {
        headers: {
          Authorization: `Bearer`,
          'Content-Type': 'application/octet-stream',
        }
      }
    );

    if (uploadResponse.status === 204) {
      res.status(200).json({ message: '✅ File uploaded to Unity Catalog. Running job...' });
      lastUploadedFile = req.file.originalname;
      triggerAndMonitorJob(sendLogToClients, lastUploadedFile, finalCatalog, finalSchema); // pass CSV name
      // triggerAndMonitorJob(sendLogToClients);
    } else {
      res.status(500).json({ error: '⚠️ Unexpected Databricks response' });
    }
  } catch (err) {
    console.error('Upload Error:', err.message);
    res.status(500).json({ error: err.message });
  }
});


app.get('/fetch-all-logs/:catalog/:schema/:csvFile', async (req, res) => {
  const { catalog, schema, csvFile } = req.params;

  try {
    const usecases = await extractUsecasesFromCSV(csvFile);
    const logs = await fetchAllLogsAndResultsForUsecases({ catalog, schema, usecases });

    res.status(200).json({ usecases, logs });
  } catch (err) {
    console.error('❌ Error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// === Start Server ===
app.listen(port, () => {
  console.log(`🚀 Server running at http://localhost:${port}`);
});
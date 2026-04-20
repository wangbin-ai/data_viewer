const express = require('express');
const fs = require('fs');
const path = require('path');
const readline = require('readline');

const app = express();
const PORT = 3000;

app.use(express.static('public'));

function safePath(p) {
  // Normalize and reject null bytes
  if (!p || p.includes('\0')) return null;
  return path.normalize(p);
}

// Browse a directory
app.get('/api/browse', (req, res) => {
  const dirPath = safePath(req.query.path);
  if (!dirPath) return res.status(400).json({ error: 'Invalid path' });

  try {
    const stat = fs.statSync(dirPath);
    if (!stat.isDirectory()) return res.status(400).json({ error: 'Not a directory' });

    const entries = fs.readdirSync(dirPath, { withFileTypes: true });
    const items = entries
      .filter(e => e.isDirectory() || e.name.endsWith('.jsonl'))
      .map(e => ({
        name: e.name,
        isDir: e.isDirectory(),
        path: path.join(dirPath, e.name),
      }))
      .sort((a, b) => {
        if (a.isDir !== b.isDir) return a.isDir ? -1 : 1;
        return a.name.localeCompare(b.name);
      });

    const hasJsonlDir = fs.existsSync(path.join(dirPath, 'jsonl')) &&
      fs.statSync(path.join(dirPath, 'jsonl')).isDirectory();
    const hasImagesDir = fs.existsSync(path.join(dirPath, 'images')) &&
      fs.statSync(path.join(dirPath, 'images')).isDirectory();

    res.json({
      path: dirPath,
      items,
      isDataDir: hasJsonlDir && hasImagesDir,
    });
  } catch (e) {
    res.status(400).json({ error: e.message });
  }
});

// List jsonl files inside a data dir
app.get('/api/jsonl-files', (req, res) => {
  const dirPath = safePath(req.query.path);
  if (!dirPath) return res.status(400).json({ error: 'Invalid path' });

  try {
    const jsonlDir = path.join(dirPath, 'jsonl');
    const files = fs.readdirSync(jsonlDir)
      .filter(f => f.endsWith('.jsonl'))
      .sort()
      .map(f => ({ name: f, path: path.join(jsonlDir, f) }));
    res.json({ files });
  } catch (e) {
    res.status(400).json({ error: e.message });
  }
});

// Read records from a jsonl file with offset/limit pagination
app.get('/api/records', async (req, res) => {
  const filePath = safePath(req.query.file);
  if (!filePath) return res.status(400).json({ error: 'Invalid path' });

  const offset = Math.max(0, parseInt(req.query.offset) || 0);
  const limit = Math.min(50, Math.max(1, parseInt(req.query.limit) || 20));

  try {
    const records = [];
    let lineNum = 0;
    let hasMore = false;

    const rl = readline.createInterface({
      input: fs.createReadStream(filePath),
      crlfDelay: Infinity,
    });

    for await (const line of rl) {
      const trimmed = line.trim();
      if (!trimmed) continue;

      if (lineNum >= offset && lineNum < offset + limit) {
        records.push({ lineNum, raw: trimmed });
      } else if (lineNum >= offset + limit) {
        hasMore = true;
        rl.close();
        break;
      }
      lineNum++;
    }

    res.json({ records, offset, hasMore, total: lineNum + (hasMore ? 1 : 0) });
  } catch (e) {
    res.status(400).json({ error: e.message });
  }
});

// Serve an image file by absolute path
app.get('/api/image', (req, res) => {
  const imgPath = safePath(req.query.path);
  if (!imgPath) return res.status(400).json({ error: 'Invalid path' });

  try {
    if (!fs.existsSync(imgPath)) return res.status(404).send('Not found');
    res.sendFile(imgPath);
  } catch (e) {
    res.status(400).json({ error: e.message });
  }
});

app.listen(PORT, () => {
  console.log(`Data Viewer running at http://localhost:${PORT}`);
});

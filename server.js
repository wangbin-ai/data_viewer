const express = require('express');
const fs = require('fs');
const path = require('path');
const readline = require('readline');
const os = require('os');
const tar = require('tar');

const app = express();
const PORT = 3008;

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
    const hasJsonlFiles = entries.some(e => !e.isDirectory() && e.name.endsWith('.jsonl'));

    res.json({
      path: dirPath,
      items,
      isDataDir: hasJsonlDir || hasJsonlFiles,
      hasImagesDir,
    });
  } catch (e) {
    res.status(400).json({ error: e.message });
  }
});

// List jsonl files: jsonl/ subdir takes priority, otherwise current dir
app.get('/api/jsonl-files', (req, res) => {
  const dirPath = safePath(req.query.path);
  if (!dirPath) return res.status(400).json({ error: 'Invalid path' });

  try {
    const subdir = path.join(dirPath, 'jsonl');
    const jsonlDir = fs.existsSync(subdir) && fs.statSync(subdir).isDirectory()
      ? subdir : dirPath;
    const files = fs.readdirSync(jsonlDir)
      .filter(f => f.endsWith('.jsonl')).sort()
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

// ── Tar image cache ──
const CACHE_DIR = path.join(os.tmpdir(), 'data_viewer_cache');
fs.mkdirSync(CACHE_DIR, { recursive: true });

// imagesDir → Promise<Map<filename, tarFilePath>>
const dirIndexPromises = new Map();
// tarFilePath → Promise<cacheDir>
const tarExtractPromises = new Map();

function buildDirIndex(imagesDir) {
  if (dirIndexPromises.has(imagesDir)) return dirIndexPromises.get(imagesDir);

  const promise = (async () => {
    const index = new Map();
    let entries;
    try { entries = fs.readdirSync(imagesDir); } catch { return index; }

    const tarFiles = entries.filter(f => /\.(tar|tar\.gz|tgz)$/i.test(f));
    for (const tf of tarFiles) {
      const tarPath = path.join(imagesDir, tf);
      try {
        await tar.list({
          file: tarPath,
          onentry(entry) {
            const base = path.basename(entry.path);
            if (base && !index.has(base)) index.set(base, tarPath);
          },
        });
      } catch { /* skip unreadable tar */ }
    }
    return index;
  })();

  dirIndexPromises.set(imagesDir, promise);
  return promise;
}

function extractTar(tarPath) {
  if (tarExtractPromises.has(tarPath)) return tarExtractPromises.get(tarPath);

  const safeKey = Buffer.from(tarPath).toString('base64').replace(/[/+=]/g, '_');
  const outDir = path.join(CACHE_DIR, safeKey);
  fs.mkdirSync(outDir, { recursive: true });

  const promise = tar.extract({ file: tarPath, cwd: outDir })
    .then(() => outDir)
    .catch(e => { tarExtractPromises.delete(tarPath); throw e; });

  tarExtractPromises.set(tarPath, promise);
  return promise;
}

function findFileRecursive(dir, name) {
  for (const e of fs.readdirSync(dir, { withFileTypes: true })) {
    const fp = path.join(dir, e.name);
    if (e.isDirectory()) {
      const found = findFileRecursive(fp, name);
      if (found) return found;
    } else if (e.name === name) {
      return fp;
    }
  }
  return null;
}

// Serve an image.
// Accepts ?base=<images-dir>&rel=<relative-path-inside-images-dir>
// Falls back to tar extraction when the file isn't on disk directly.
app.get('/api/image', async (req, res) => {
  const base = safePath(req.query.base);   // e.g. /root/data/stem/images
  const rel  = req.query.rel;              // e.g. 新能源分析/257.png
  if (!base || !rel) return res.status(400).json({ error: 'Missing base or rel param' });

  const imgPath = path.join(base, rel);
  const filename = path.basename(rel);

  // Serve directly if the file already exists on disk
  if (fs.existsSync(imgPath)) return res.sendFile(imgPath);

  // Search for the file inside tar archives in the base images directory
  try {
    const index = await buildDirIndex(base);
    const tarPath = index.get(filename);
    if (!tarPath) return res.status(404).send('Image not found in any tar');

    const cacheDir = await extractTar(tarPath);
    const found = findFileRecursive(cacheDir, filename);
    if (!found) return res.status(404).send('File missing after extraction');

    res.sendFile(found);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.listen(PORT, () => {
  console.log(`Data Viewer running at http://localhost:${PORT}`);
});

const express = require('express');
const fs = require('fs');
const path = require('path');
const readline = require('readline');
const os = require('os');
const tar = require('tar');

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

// Evict the oldest entry when a Map exceeds maxSize (FIFO)
function mapSet(map, key, value, maxSize = 30) {
  if (!map.has(key) && map.size >= maxSize)
    map.delete(map.keys().next().value);
  map.set(key, value);
}

// imagesDir → Promise<Map<relPath | '$'+basename, tarFilePath>>
const dirIndexPromises = new Map();
// tarFilePath → Promise<cacheDir>  (never evicted; resolved promises are tiny)
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
            // Normalize: strip leading ./ and use forward slashes
            const rel = entry.path.replace(/^\.\//, '').replace(/\\/g, '/');
            if (!rel || rel.endsWith('/')) return;            // skip dirs
            if (!index.has(rel)) index.set(rel, tarPath);    // exact rel path
            const fbKey = '$' + path.basename(rel);
            if (!index.has(fbKey)) index.set(fbKey, tarPath); // basename fallback
          },
        });
      } catch { /* skip unreadable tar */ }
    }
    return index;
  })();

  mapSet(dirIndexPromises, imagesDir, promise);
  return promise;
}

function extractTar(tarPath) {
  if (tarExtractPromises.has(tarPath)) return tarExtractPromises.get(tarPath);

  const safeKey = Buffer.from(tarPath).toString('base64').replace(/[/+=]/g, '_');
  const outDir = path.join(CACHE_DIR, safeKey);
  const marker = path.join(outDir, '.done');

  // Disk cache already complete — skip extraction, re-add to Map
  if (fs.existsSync(marker)) {
    const p = Promise.resolve(outDir);
    tarExtractPromises.set(tarPath, p);
    return p;
  }

  fs.mkdirSync(outDir, { recursive: true });

  const promise = tar.extract({ file: tarPath, cwd: outDir })
    .then(() => { fs.writeFileSync(marker, ''); return outDir; })
    .catch(e => { tarExtractPromises.delete(tarPath); throw e; });

  // No size cap: a resolved Promise uses only ~a few hundred bytes
  tarExtractPromises.set(tarPath, promise);
  return promise;
}

function findFileRecursive(dir, name) {
  for (const e of fs.readdirSync(dir, { withFileTypes: true })) {
    if (e.name === '.done') continue;
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

// ── Tarinfo-based image access ──
// Cache: tarinfPath → Promise<Object>
const tarinfoCache = new Map();

async function loadTarinfo(tarinfPath) {
  if (tarinfoCache.has(tarinfPath)) return tarinfoCache.get(tarinfPath);
  const p = fs.promises.readFile(tarinfPath, 'utf8')
    .then(JSON.parse)
    .catch(() => null);
  mapSet(tarinfoCache, tarinfPath, p, 50);
  return p;
}

// Given a jsonlBase like "0000001", find the matching tar in imagesDir.
// Tries: exact base.tar → numeric suffix match against part_NNN.tar etc.
function findTarForBase(imagesDir, base) {
  const exact = path.join(imagesDir, base + '.tar');
  if (fs.existsSync(exact)) return exact;

  // Extract trailing number from base, match against any tar's number
  const numMatch = base.match(/(\d+)$/);
  if (!numMatch) return null;
  const num = parseInt(numMatch[1], 10);

  let entries;
  try { entries = fs.readdirSync(imagesDir); } catch { return null; }

  for (const f of entries) {
    if (!/\.(tar|tar\.gz|tgz)$/i.test(f)) continue;
    const m = f.match(/(\d+)/);
    if (m && parseInt(m[1], 10) === num) return path.join(imagesDir, f);
  }
  return null;
}

// Read raw image bytes from a tar at a specific byte offset (no full extraction needed).
async function extractImageByOffset(tarFile, offsetData, size) {
  const buf = Buffer.alloc(size);
  const handle = await fs.promises.open(tarFile, 'r');
  try {
    await handle.read(buf, 0, size, offsetData);
  } finally {
    await handle.close();
  }
  return buf;
}

const MIME = { png:'image/png', jpg:'image/jpeg', jpeg:'image/jpeg',
               gif:'image/gif', webp:'image/webp', bmp:'image/bmp' };

// Serve an image.
// ?base=<images-dir>  &rel=<relative-path>  [&jsonlBase=<jsonl-stem>]
//
// Priority:
//  1. File exists directly on disk
//  2. Tarinfo-based offset read (new format, requires jsonlBase)
//  3. Full-tar extraction + path lookup (original format)
app.get('/api/image', async (req, res) => {
  const base      = safePath(req.query.base);
  const rel       = (req.query.rel || '').replace(/\\/g, '/').replace(/^\.\//, '');
  const jsonlBase = (req.query.jsonlBase || '').trim();
  if (!base || !rel) return res.status(400).json({ error: 'Missing base or rel param' });

  const filename = path.basename(rel);
  const imgPath  = path.join(base, rel);

  // 1. Direct file on disk
  if (fs.existsSync(imgPath)) return res.sendFile(imgPath);

  // 2. Tarinfo offset-based read
  if (jsonlBase) {
    const tarinfPath = path.join(base, jsonlBase + '_tarinfo.json');
    if (fs.existsSync(tarinfPath)) {
      try {
        const tarinfo = await loadTarinfo(tarinfPath);
        const entry   = tarinfo?.[rel] ?? tarinfo?.[filename];
        if (entry?.offset_data != null && entry?.size) {
          const tarFile = findTarForBase(base, jsonlBase);
          if (tarFile) {
            const imgData = await extractImageByOffset(tarFile, entry.offset_data, entry.size);
            const ext  = path.extname(filename).toLowerCase().slice(1);
            res.setHeader('Content-Type', MIME[ext] || 'application/octet-stream');
            return res.send(imgData);
          }
        }
      } catch { /* fall through */ }
    }
  }

  // 3. Full-tar extraction (original format)
  try {
    const index = await buildDirIndex(base);
    const tarPath = index.get(rel) || index.get('$' + filename);
    if (!tarPath) return res.status(404).send('Image not found in any tar');

    const cacheDir = await extractTar(tarPath);
    const direct   = path.join(cacheDir, rel);
    if (fs.existsSync(direct)) return res.sendFile(direct);

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

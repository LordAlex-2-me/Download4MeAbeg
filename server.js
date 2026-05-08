'use strict';

const express      = require('express');
const axios        = require('axios');
const fs           = require('fs');
const path         = require('path');
const os           = require('os');
const { spawn }    = require('child_process');
const { v4: uuidv4 } = require('uuid');
const archiver     = require('archiver');
const torrentStream = require('torrent-stream');
const { S3Client, PutObjectCommand, GetObjectCommand, DeleteObjectCommand } = require('@aws-sdk/client-s3');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');

const app = express();
app.use(express.json());
app.use(express.static('public'));

// ── Startup validation ─────────────────────────────────────────────────────────
// Fail loudly at boot if any required R2 variable is missing or blank.
// This surfaces the problem in the Render deploy logs immediately, rather than
// letting the server start and fail silently on the first actual download attempt.
const REQUIRED_ENV = ['R2_ACCOUNT_ID', 'R2_ACCESS_KEY', 'R2_SECRET_KEY', 'R2_BUCKET'];
const missing = REQUIRED_ENV.filter(k => !process.env[k] || !process.env[k].trim());
if (missing.length) {
  console.error('=== Download4MePls: MISSING ENVIRONMENT VARIABLES ===');
  console.error(`The following required variables are not set: ${missing.join(', ')}`);
  console.error('Set them in Render dashboard -> Your Service -> Environment.');
  console.error('The server will not start until all four R2 variables are present.');
  process.exit(1);
}

// ── R2 client ──────────────────────────────────────────────────────────────────
// Trim all values defensively — copy-pasting keys from Cloudflare sometimes
// introduces a trailing newline or space, which makes the SDK reject them.
const r2 = new S3Client({
  region: 'auto',
  endpoint: `https://${process.env.R2_ACCOUNT_ID.trim()}.r2.cloudflarestorage.com`,
  credentials: {
    accessKeyId:     process.env.R2_ACCESS_KEY.trim(),
    secretAccessKey: process.env.R2_SECRET_KEY.trim(),
  },
});

const R2_BUCKET = process.env.R2_BUCKET.trim();

// ── Job store (in-memory) ──────────────────────────────────────────────────────
const jobs = {};

// ── Constants ──────────────────────────────────────────────────────────────────
const TMP_DIR = path.join(os.tmpdir(), 'd4mp');
if (!fs.existsSync(TMP_DIR)) fs.mkdirSync(TMP_DIR, { recursive: true });

const YTDLP_CANDIDATES = [
  'yt-dlp',
  '/usr/local/bin/yt-dlp',
  `${os.homedir()}/.local/bin/yt-dlp`,
  '/usr/bin/yt-dlp',
];

const VIDEO_HOSTNAMES = [
  'youtube.com', 'youtu.be', 'vimeo.com', 'dailymotion.com',
  'twitter.com', 'x.com', 'instagram.com', 'tiktok.com',
  'facebook.com', 'reddit.com', 'twitch.tv', 'streamable.com',
  'bilibili.com', 'nicovideo.jp', 'soundcloud.com', 'bandcamp.com',
];

// ── Helpers ────────────────────────────────────────────────────────────────────
function safeFilename(name) {
  return String(name).replace(/[^a-zA-Z0-9._\-]/g, '_').slice(0, 200);
}

function guessFilename(url) {
  try {
    const base = path.basename(new URL(url).pathname);
    return base && base !== '/' ? safeFilename(base) : 'download';
  } catch { return 'download'; }
}

function formatBytes(b) {
  if (!b) return null;
  if (b < 1024) return `${b} B`;
  if (b < 1024 ** 2) return `${(b / 1024).toFixed(1)} KB`;
  if (b < 1024 ** 3) return `${(b / 1024 ** 2).toFixed(2)} MB`;
  return `${(b / 1024 ** 3).toFixed(2)} GB`;
}

function detectType(url) {
  if (!url) return 'http';
  if (url.startsWith('magnet:') || url.toLowerCase().endsWith('.torrent')) return 'torrent';
  try {
    const host = new URL(url).hostname.replace(/^www\./, '');
    if (VIDEO_HOSTNAMES.some(h => host === h || host.endsWith('.' + h))) return 'ytdlp';
  } catch { /* not a standard URL */ }
  return 'http';
}

// ── R2 operations ──────────────────────────────────────────────────────────────
async function uploadStreamToR2(key, stream, contentType = 'application/octet-stream') {
  await r2.send(new PutObjectCommand({
    Bucket: R2_BUCKET,
    Key: key,
    Body: stream,
    ContentType: contentType,
  }));
}

async function uploadFileToR2(key, filePath) {
  const ext  = path.extname(filePath).toLowerCase();
  const mime = { '.mp4': 'video/mp4', '.mp3': 'audio/mpeg', '.zip': 'application/zip',
                 '.pdf': 'application/pdf', '.mkv': 'video/x-matroska' }[ext] || 'application/octet-stream';
  const stream = fs.createReadStream(filePath);
  await uploadStreamToR2(key, stream, mime);
}

async function getSignedDownloadUrl(key, filename) {
  return getSignedUrl(r2, new GetObjectCommand({
    Bucket: R2_BUCKET,
    Key: key,
    ResponseContentDisposition: `attachment; filename="${filename}"`,
  }), { expiresIn: 3600 });
}

async function deleteFromR2(key) {
  try {
    await r2.send(new DeleteObjectCommand({ Bucket: R2_BUCKET, Key: key }));
  } catch { /* best-effort */ }
}

// ── Download handlers ──────────────────────────────────────────────────────────

// 1. Regular HTTP download — stream directly to R2
async function downloadHttp(jobId, url, credentials) {
  const job = jobs[jobId];
  const config = {
    method: 'GET', url,
    responseType: 'stream',
    timeout: 20 * 60 * 1000,
    maxRedirects: 10,
    headers: { 'User-Agent': 'Download4MePls/2.0' },
  };

  const c = credentials;
  if (c) {
    if (c.type === 'basic')  config.auth = { username: c.username, password: c.password };
    if (c.type === 'bearer') config.headers['Authorization'] = `Bearer ${c.token}`;
    if (c.type === 'cookie') config.headers['Cookie'] = c.value;
    if (c.type === 'header') config.headers[c.name] = c.value;
  }

  const response = await axios(config);

  // Detect filename from Content-Disposition
  const cd = response.headers['content-disposition'];
  if (cd) {
    const m = cd.match(/filename\*?=(?:UTF-8'')?["']?([^"';\n]+)/i);
    if (m) job.filename = safeFilename(decodeURIComponent(m[1].trim()));
  }

  const cl = parseInt(response.headers['content-length']);
  if (cl) job.totalBytes = cl;

  const ct = response.headers['content-type']?.split(';')[0] || 'application/octet-stream';

  let bytesWritten = 0;
  response.data.on('data', chunk => {
    bytesWritten += chunk.length;
    job.bytesDownloaded = bytesWritten;
    if (job.totalBytes) job.progress = Math.round((bytesWritten / job.totalBytes) * 100);
  });

  await uploadStreamToR2(jobId, response.data, ct);
}

// 2. yt-dlp download — spawn process, upload output file to R2
async function downloadYtdlp(jobId, url, credentials) {
  const job = jobs[jobId];
  const outTemplate = path.join(TMP_DIR, `${jobId}.%(ext)s`);

  const args = [
    url,
    '-o', outTemplate,
    '--no-playlist',
    '--merge-output-format', 'mp4',
    '--newline',
    '--js-runtimes', 'node',  // use the server's Node.js runtime for YouTube extraction
  ];

  // Pass cookies if provided
  if (credentials?.type === 'cookie') {
    const cookieFile = path.join(TMP_DIR, `${jobId}.txt`);
    fs.writeFileSync(cookieFile, credentials.value);
    args.push('--cookies', cookieFile);
  }

  // Find yt-dlp binary
  let ytdlpBin = null;
  for (const candidate of YTDLP_CANDIDATES) {
    try {
      const test = spawn(candidate, ['--version']);
      await new Promise((res, rej) => {
        test.on('close', code => code === 0 ? res() : rej());
        test.on('error', rej);
      });
      ytdlpBin = candidate;
      break;
    } catch { /* try next */ }
  }
  if (!ytdlpBin) throw new Error('yt-dlp is not installed on this server. Add "pip3 install yt-dlp" to your build command.');

  let outputFile = null;

  await new Promise((resolve, reject) => {
    const proc = spawn(ytdlpBin, args);

    proc.stdout.on('data', data => {
      const text = data.toString();

      // Parse progress percentage
      const pct = text.match(/(\d+\.?\d*)%/);
      if (pct) job.progress = Math.min(95, parseFloat(pct[1]));

      // Capture output filename
      const dest = text.match(/\[download\] Destination: (.+)/);
      if (dest) outputFile = dest[1].trim();
      const merge = text.match(/\[Merger\] Merging formats into "(.+)"/);
      if (merge) outputFile = merge[1].trim();
    });

    let stderr = '';
    proc.stderr.on('data', d => { stderr += d.toString(); });

    proc.on('close', code => {
      if (code !== 0) return reject(new Error(`yt-dlp failed (exit ${code}): ${stderr.slice(0, 300)}`));
      resolve();
    });
    proc.on('error', err => reject(new Error(`Could not spawn yt-dlp: ${err.message}`)));
  });

  // Fallback: scan TMP_DIR for any file starting with jobId
  if (!outputFile || !fs.existsSync(outputFile)) {
    const found = fs.readdirSync(TMP_DIR).filter(f => f.startsWith(jobId) && !f.endsWith('.txt'));
    if (!found.length) throw new Error('yt-dlp completed but no output file was found.');
    outputFile = path.join(TMP_DIR, found[0]);
  }

  job.filename = safeFilename(path.basename(outputFile));
  job.size = fs.statSync(outputFile).size;
  job.sizeFormatted = formatBytes(job.size);
  job.progress = 99;

  await uploadFileToR2(jobId, outputFile);

  // Clean up temp files
  fs.readdirSync(TMP_DIR).filter(f => f.startsWith(jobId)).forEach(f => {
    try { fs.unlinkSync(path.join(TMP_DIR, f)); } catch { /* ignore */ }
  });
}

// 3. Torrent download — torrent-stream engine, upload to R2
async function downloadTorrent(jobId, url, credentials) {
  const job = jobs[jobId];

  // Fetch .torrent file if URL (magnet links are passed directly)
  let torrentInput = url;
  if (!url.startsWith('magnet:')) {
    const res = await axios({ url, responseType: 'arraybuffer' });
    torrentInput = Buffer.from(res.data);
  }

  const torrentDir = path.join(TMP_DIR, jobId);
  if (!fs.existsSync(torrentDir)) fs.mkdirSync(torrentDir, { recursive: true });

  await new Promise((resolve, reject) => {
    const engine = torrentStream(torrentInput, { path: torrentDir });

    engine.on('error', err => { engine.destroy(); reject(err); });

    engine.on('ready', () => {
      const torrent = engine.torrent;
      job.filename    = safeFilename(torrent.name || 'torrent');
      job.totalBytes  = torrent.length;
      job.torrentInfo = { files: torrent.files.map(f => ({ name: f.name, length: f.length })) };

      // Start downloading all pieces
      engine.files.forEach(f => f.select());

      let lastLogged = 0;
      const interval = setInterval(() => {
        const downloaded = engine.swarm?.downloaded ?? 0;
        job.bytesDownloaded = downloaded;
        if (job.totalBytes) job.progress = Math.round((downloaded / job.totalBytes) * 100);
        job.torrentInfo.peers = engine.swarm?.wires?.length ?? 0;

        if (job.progress === 100 && lastLogged === 100) {
          clearInterval(interval);
        }
        lastLogged = job.progress;
      }, 1500);

      engine.on('idle', async () => {
        clearInterval(interval);
        job.progress = 99;

        try {
          const files = engine.files;
          let uploadPath, uploadName;

          if (files.length === 1) {
            // Single file — upload directly
            uploadPath = path.join(torrentDir, files[0].path);
            uploadName = safeFilename(files[0].name);
          } else {
            // Multiple files — zip them
            uploadName = safeFilename(torrent.name) + '.zip';
            uploadPath = path.join(TMP_DIR, `${jobId}.zip`);
            await zipDirectory(torrentDir, uploadPath);
          }

          job.filename = uploadName;
          job.size = fs.statSync(uploadPath).size;
          job.sizeFormatted = formatBytes(job.size);

          await uploadFileToR2(jobId, uploadPath);

          engine.destroy();
          // Cleanup
          try { fs.rmSync(torrentDir, { recursive: true }); } catch {}
          if (files.length > 1) { try { fs.unlinkSync(uploadPath); } catch {} }

          resolve();
        } catch (err) {
          engine.destroy();
          reject(err);
        }
      });
    });
  });
}

function zipDirectory(sourceDir, destPath) {
  return new Promise((resolve, reject) => {
    const output = fs.createWriteStream(destPath);
    const archive = archiver('zip', { zlib: { level: 6 } });
    output.on('close', resolve);
    archive.on('error', reject);
    archive.pipe(output);
    archive.directory(sourceDir, false);
    archive.finalize();
  });
}

// ── Main download orchestrator ────────────────────────────────────────────────
async function runDownload(jobId) {
  const job = jobs[jobId];
  if (!job) return;
  job.status = 'downloading';
  job.startedAt = new Date().toISOString();

  try {
    switch (job.type) {
      case 'ytdlp':   await downloadYtdlp(jobId, job.url, job.credentials);   break;
      case 'torrent':  await downloadTorrent(jobId, job.url, job.credentials); break;
      default:         await downloadHttp(jobId, job.url, job.credentials);    break;
    }

    if (!job.sizeFormatted && job.size) job.sizeFormatted = formatBytes(job.size);
    job.status       = 'complete';
    job.progress     = 100;
    job.completedAt  = new Date().toISOString();
  } catch (err) {
    job.status = 'error';
    job.error  = err.message;
  }
}

// ── Routes ────────────────────────────────────────────────────────────────────

// Health check — visit /api/health to confirm server is running and R2 config loaded
app.get('/api/health', (req, res) => {
  res.json({
    status: 'ok',
    r2Endpoint: `https://${process.env.R2_ACCOUNT_ID.trim()}.r2.cloudflarestorage.com`,
    r2Bucket: R2_BUCKET,
    jobCount: Object.keys(jobs).length,
  });
});

app.get('/api/jobs', (req, res) => {
  const list = Object.values(jobs)
    .map(j => ({ ...j, credentials: undefined }))
    .sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));
  res.json(list);
});

app.post('/api/jobs', (req, res) => {
  const { url, credentials, filename } = req.body;
  if (!url || typeof url !== 'string') return res.status(400).json({ error: 'A URL or magnet link is required.' });

  const type = detectType(url);

  // Validate URL format (skip for magnet links)
  if (type !== 'torrent') {
    try { new URL(url); } catch { return res.status(400).json({ error: 'Invalid URL format.' }); }
  }

  const jobId = uuidv4();
  jobs[jobId] = {
    id: jobId,
    url,
    type,
    filename: filename ? safeFilename(filename) : (type === 'torrent' ? 'torrent' : guessFilename(url)),
    credentials: credentials || null,
    status: 'pending',
    progress: 0,
    bytesDownloaded: 0,
    totalBytes: null,
    size: null,
    sizeFormatted: null,
    torrentInfo: null,
    createdAt: new Date().toISOString(),
    startedAt: null,
    completedAt: null,
    error: null,
  };

  res.status(202).json({ jobId, type });
  setImmediate(() => runDownload(jobId));
});

app.get('/api/jobs/:id', (req, res) => {
  const job = jobs[req.params.id];
  if (!job) return res.status(404).json({ error: 'Job not found.' });
  res.json({ ...job, credentials: undefined });
});

app.post('/api/jobs/:id/retry', (req, res) => {
  const job = jobs[req.params.id];
  if (!job) return res.status(404).json({ error: 'Job not found.' });
  if (job.status !== 'error') return res.status(400).json({ error: 'Only failed jobs can be retried.' });
  job.status = 'pending'; job.progress = 0; job.bytesDownloaded = 0; job.error = null;
  res.json({ jobId: job.id });
  setImmediate(() => runDownload(job.id));
});

// Generate a signed R2 URL and redirect
app.get('/api/download/:id', async (req, res) => {
  const job = jobs[req.params.id];
  if (!job) return res.status(404).json({ error: 'Job not found.' });
  if (job.status !== 'complete') return res.status(400).json({ error: 'File is not ready yet.' });
  try {
    const url = await getSignedDownloadUrl(job.id, job.filename);
    res.redirect(url);
  } catch (err) {
    res.status(500).json({ error: `Could not generate download link: ${err.message}` });
  }
});

app.delete('/api/jobs/:id', async (req, res) => {
  const job = jobs[req.params.id];
  if (!job) return res.status(404).json({ error: 'Job not found.' });
  if (job.status === 'complete') await deleteFromR2(job.id);
  delete jobs[req.params.id];
  res.json({ success: true });
});

// ── Boot ──────────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Download4MePls running on http://localhost:${PORT}`));

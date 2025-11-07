// server.js
// Minimal backend to run the client binary and return results to the frontend.
// Place this file in Client-Node/ui/

const express = require("express");
const multer = require("multer");
const path = require("path");
const fs = require("fs");
const { execFile } = require("child_process");
const cors = require("cors");

// === Paths relative to this ui/ folder ===
const CLIENT_BIN      = path.resolve("../target/release/client-node");
const CLIENT_TEST_DIR = path.resolve("../client_test");
const DECRYPTED_DIR   = path.resolve("../decrypted_data");

// Comma-separated peers (no spaces)
const PEERS = "10.40.61.79:8180,10.40.58.169:8181,10.40.63.10:8183";

// Ensure result dirs exist
fs.mkdirSync(CLIENT_TEST_DIR, { recursive: true });
fs.mkdirSync(DECRYPTED_DIR, { recursive: true });

// Upload dir for incoming PNGs
const UPLOAD_DIR = path.resolve("./uploads");
fs.mkdirSync(UPLOAD_DIR, { recursive: true });

const app = express();
app.use(cors());
app.use(express.json());

// Serve static UI from this folder
app.use(express.static(__dirname));
app.get("/", (_req, res) => {
  res.sendFile(path.join(__dirname, "index.html"));
});

// Expose result folders so browser can fetch the returned image + files
app.use("/client_test", express.static(CLIENT_TEST_DIR));
app.use("/decrypted_data", express.static(DECRYPTED_DIR));

// Multer for PNG uploads
const storage = multer.diskStorage({
  destination: (_req, _file, cb) => cb(null, UPLOAD_DIR),
  filename: (_req, file, cb) => {
    const safe = Date.now() + "-" + file.originalname.replace(/[^\w.\-]/g, "_");
    cb(null, safe);
  }
});
const upload = multer({
  storage,
  limits: { fileSize: 50 * 1024 * 1024 }, // 50MB
  fileFilter: (_req, file, cb) => {
    if (!/\.png$/i.test(file.originalname)) return cb(new Error("Only .png allowed"));
    cb(null, true);
  }
});

// Helpers: only return files created/modified since THIS run started
function newestFileSince(dir, suffix, sinceMs) {
  if (!fs.existsSync(dir)) return null;
  const files = fs.readdirSync(dir)
    .filter(f => f.endsWith(suffix))
    .map(f => {
      const p = path.join(dir, f);
      const st = fs.statSync(p);
      return { p, f, t: st.mtimeMs };
    })
    .filter(x => x.t >= sinceMs)
    .sort((a, b) => b.t - a.t);
  return files.length ? files[0].p : null;
}

function readDecryptedSince(dir, sinceMs, maxBytes = 200_000) {
  if (!fs.existsSync(dir)) return [];
  return fs.readdirSync(dir)
    .map(f => {
      const p = path.join(dir, f);
      const st = fs.statSync(p);
      return { p, f, t: st.mtimeMs };
    })
    .filter(x => x.t >= sinceMs)
    .filter(x => /\.(txt|json|log)$/i.test(x.f))
    .sort((a, b) => b.t - a.t)
    .map(x => {
      let content = "";
      try { content = fs.readFileSync(x.p, "utf8").slice(0, maxBytes); } catch {}
      const size = fs.statSync(x.p).size;
      return { file: x.f, size, content };
    });
}

// Health check
app.get("/api/health", (_req, res) => {
  res.json({ ok: true, client_bin: fs.existsSync(CLIENT_BIN), time: Date.now() });
});

// Main endpoint
app.post("/api/run-client", upload.single("image"), (req, res) => {
  try {
    const { username, password, metadata } = req.body;

    if (!username || !password) {
      return res.status(400).json({ error: "Missing credentials" });
    }
    if (!req.file) {
      return res.status(400).json({ error: "No PNG uploaded" });
    }

    const allow = String(metadata || "").trim();
    if (!/^[A-Za-z0-9_]+:\d+(,[A-Za-z0-9_]+:\d+)*$/.test(allow)) {
      return res.status(400).json({ error: "Metadata format must be like bob:5 or alice:3,bob:2" });
    }

    if (!fs.existsSync(CLIENT_BIN)) {
      return res.status(500).json({ error: `Client binary not found at ${CLIENT_BIN}. Run: cargo build --release` });
    }

    const imgPath = req.file.path;

    const args = [
      "--peers", PEERS,
      "--processes", "1",
      "--requests-per-user", "1",
      "--image", imgPath,
      "--allow", allow
    ];

    // Mark start of this run (slight buffer)
    const startedAt = Date.now() - 50;

    execFile(CLIENT_BIN, args, { cwd: path.resolve("..") }, (err, stdout, stderr) => {
      const finishedAt = Date.now();

      const newestEncrypted = newestFileSince(CLIENT_TEST_DIR, "_encrypted.png", startedAt);
      const decrypted = readDecryptedSince(DECRYPTED_DIR, startedAt);

      const payload = {
        ok: !err,
        duration_ms: finishedAt - startedAt,
        stdout,
        stderr,
        image_url: newestEncrypted ? `/client_test/${path.basename(newestEncrypted)}` : null,
        decrypted_files: decrypted
      };

      if (err) {
        payload.error = err.message || String(err);
        return res.status(500).json(payload);
      }
      res.json(payload);
    });
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// Error handler (including Multer)
app.use((err, _req, res, _next) => {
  if (err && err.message === "Only .png allowed") {
    return res.status(400).json({ error: err.message });
  }
  if (err) return res.status(500).json({ error: String(err) });
  res.status(404).json({ error: "Not found" });
});

const PORT = 3000;
app.listen(PORT, () => {
  console.log(`Server on http://localhost:${PORT}`);
});

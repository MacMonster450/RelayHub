const express = require("express");
const http = require("http");
const WebSocket = require("ws");

const app = express();
const server = http.createServer(app);

// Keep proxied HTTP connections stable during longer ESP32 operations.
server.keepAliveTimeout = 120000;
server.headersTimeout = 125000;

const wss = new WebSocket.Server({
    server,
    path: "/tunnel",
    maxPayload: 2 * 1024 * 1024
});

let esp32 = null;
const pending = new Map();

// Serialize HTTP requests through the single WebServer instance on the ESP32.
// A browser refresh can create many parallel requests (HTML/CSS/JS/images).
// The ESP32 can safely process only one tunnel request at a time, so Render
// queues them instead of sending "ESP32 tunnel is busy" responses.
const espQueue = [];
let espBusy = false;
let activeEspJob = null;

// WebSocket heartbeat/health tracking.
const HEARTBEAT_INTERVAL = 30000;
const HEARTBEAT_TIMEOUT = 10000;

function clearPending(id) {
    const item = pending.get(id);
    if (!item) return null;
    clearTimeout(item.timer);
    pending.delete(id);
    return item;
}

function finishEspJob(id) {
    if (activeEspJob && activeEspJob.id === id) {
        activeEspJob = null;
        espBusy = false;
        pumpEspQueue();
    }
}

function failPending(id, status, message) {
    const item = clearPending(id);
    if (!item) {
        finishEspJob(id);
        return;
    }

    if (!item.res.headersSent) {
        item.res.status(status).send(message);
    } else {
        try { item.res.end(); } catch {}
    }

    finishEspJob(id);
}

function queueEspJob(job) {
    espQueue.push(job);
    pumpEspQueue();
}

async function pumpEspQueue() {
    if (espBusy) return;

    if (!esp32 || esp32.readyState !== WebSocket.OPEN) {
        return;
    }

    while (espQueue.length > 0) {
        const job = espQueue.shift();
        if (!job) return;

        if (!pending.has(job.id)) {
            continue;
        }

        espBusy = true;
        activeEspJob = job;

        try {
            await job.send(esp32);
        } catch (error) {
            console.error(
                "[WS] Failed to send queued job:",
                error.message
            );

            failPending(
                job.id,
                502,
                "Failed to send request to ESP32"
            );
        }

        // Normal jobs release the ESP slot when their response arrives.
        // If the job failed here, failPending() already released it.
        if (activeEspJob && activeEspJob.id === job.id) {
            return;
        }

        if (!esp32 || esp32.readyState !== WebSocket.OPEN) {
            return;
        }

        if (espBusy) {
            return;
        }
    }
}

function sendPendingResponse(id, item, body) {
    if (!item || item.finished) return;

    item.finished = true;
    clearTimeout(item.timer);
    pending.delete(id);

    const headers = item.headers || {};

    for (const [key, value] of Object.entries(headers)) {
        const lower = key.toLowerCase();

        if (
            lower === "connection" ||
            lower === "content-length" ||
            lower === "transfer-encoding"
        ) {
            continue;
        }

        if (value === undefined || value === null) {
            continue;
        }

        if (lower === "set-cookie") {
            item.res.setHeader(
                "Set-Cookie",
                Array.isArray(value) ? value.map(String) : [String(value)]
            );
        } else if (Array.isArray(value)) {
            item.res.setHeader(
                key,
                value.map(String)
            );
        } else {
            item.res.setHeader(
                key,
                String(value)
            );
        }
    }

    item.res.setHeader("Content-Length", body.length);
    item.res.setHeader(
        "Cache-Control",
        headers["cache-control"] || "no-store"
    );

    const statusCode = Number(item.statusCode || 200);

    console.log(
        "[HTTP] -> Browser",
        statusCode,
        "bytes:",
        body.length,
        "id:",
        id
    );

    item.res.status(statusCode).end(body);
    finishEspJob(id);
}

function sendWs(ws, payload) {
    return new Promise((resolve, reject) => {
        if (!ws || ws.readyState !== WebSocket.OPEN) {
            reject(new Error("ESP32 WebSocket is not connected"));
            return;
        }

        ws.send(JSON.stringify(payload), (err) => {
            if (err) reject(err);
            else resolve();
        });
    });
}

// ------------------------------------------------------------
// Multipart parser for the existing /upload form.
// The ESP32's normal local /upload route is untouched.
// Cloud uploads use a dedicated WebSocket file-transfer path.
// ------------------------------------------------------------

function parseMultipartFile(body, contentType) {
    if (!Buffer.isBuffer(body)) {
        throw new Error("Upload body is not a Buffer");
    }

    const match = /boundary=(?:"([^"]+)"|([^;]+))/i.exec(contentType || "");
    if (!match) {
        throw new Error("Multipart boundary not found");
    }

    const boundary = match[1] || match[2];
    const marker = Buffer.from(`--${boundary}`, "utf8");

    let cursor = body.indexOf(marker);

    while (cursor >= 0) {
        const partStart = cursor + marker.length;

        // End of multipart body.
        if (
            body.slice(partStart, partStart + 2).toString() === "--"
        ) {
            break;
        }

        let headerStart = partStart;

        if (
            body[headerStart] === 0x0d &&
            body[headerStart + 1] === 0x0a
        ) {
            headerStart += 2;
        }

        const headerEnd = body.indexOf(
            Buffer.from("\r\n\r\n"),
            headerStart
        );

        if (headerEnd < 0) {
            throw new Error("Incomplete multipart headers");
        }

        const headersText = body
            .slice(headerStart, headerEnd)
            .toString("utf8");

        const disposition = /content-disposition:\s*([^\r\n]+)/i.exec(
            headersText
        );

        if (disposition) {
            const value = disposition[1];
            const filenameMatch = /filename="([^"]*)"/i.exec(value);

            if (filenameMatch) {
                const filename = filenameMatch[1];
                const dataStart = headerEnd + 4;

                const nextBoundary = body.indexOf(
                    Buffer.from(`\r\n--${boundary}`),
                    dataStart
                );

                if (nextBoundary < 0) {
                    throw new Error("Multipart closing boundary not found");
                }

                const fileData = body.slice(
                    dataStart,
                    nextBoundary
                );

                return {
                    filename,
                    data: fileData
                };
            }
        }

        cursor = body.indexOf(marker, partStart);
    }

    throw new Error("No file found in multipart upload");
}

async function handleCloudUpload(req, res) {
    console.log(
        "[CLOUD UPLOAD] Request:",
        req.method,
        req.originalUrl
    );

    if (
        !esp32 ||
        esp32.readyState !== WebSocket.OPEN
    ) {
        return res
            .status(503)
            .send("ESP32 is not connected");
    }

    if (!req.body || !Buffer.isBuffer(req.body)) {
        return res
            .status(400)
            .send("Upload body missing");
    }

    let file;

    try {
        file = parseMultipartFile(
            req.body,
            req.headers["content-type"] || ""
        );
    } catch (error) {
        console.error(
            "[CLOUD UPLOAD] Multipart parse error:",
            error.message
        );

        return res
            .status(400)
            .send(error.message);
    }

    const id =
        `${Date.now()}-${Math.random()
            .toString(36)
            .slice(2, 10)}`;

    const cookie =
        String(req.headers.cookie || "");

    const timer = setTimeout(() => {
        const item = pending.get(id);
        if (!item) return;

        pending.delete(id);
        finishEspJob(id);

        if (!res.headersSent) {
            res.status(504).send("ESP32 upload timeout");
        }

        console.log(
            "[CLOUD UPLOAD] Timeout:",
            id
        );
    }, 60000);

    pending.set(id, {
        res,
        timer,
        finished: false,
        statusCode: 200,
        headers: {},
        chunks: []
    });

    // Queue the complete upload transfer so a browser refresh cannot
    // interleave upload messages with another HTTP request.
    queueEspJob({
        id,
        kind: "upload",
        async send(ws) {
            console.log(
                "[CLOUD UPLOAD] File:",
                file.filename,
                "bytes:",
                file.data.length
            );

            await sendWs(ws, {
                type: "upload_start",
                id,
                filename: file.filename,
                cookie
            });

            // Keep chunks small enough for ESP32 WebSocket memory.
            const CHUNK_SIZE = 768;

            let sequence = 0;

            for (
                let offset = 0;
                offset < file.data.length;
                offset += CHUNK_SIZE
            ) {
                const chunk = file.data.slice(
                    offset,
                    Math.min(
                        offset + CHUNK_SIZE,
                        file.data.length
                    )
                );

                await sendWs(ws, {
                    type: "upload_chunk",
                    id,
                    seq: sequence++,
                    body: chunk.toString("base64")
                });
            }

            await sendWs(ws, {
                type: "upload_end",
                id,
                totalBytes: file.data.length
            });
        }
    });
}

// ============================================================
// CLOUD OTA
// ============================================================
// Browser sends POST /api/ota as multipart/form-data.
// Render extracts the .bin and forwards it as small WebSocket
// messages so the ESP32 never receives a huge base64 JSON payload.
// ============================================================

async function handleCloudOTA(req, res) {
    console.log(
        "[CLOUD OTA] Request:",
        req.method,
        req.originalUrl
    );

    if (
        !esp32 ||
        esp32.readyState !== WebSocket.OPEN
    ) {
        return res
            .status(503)
            .send("ESP32 is not connected");
    }

    if (
        !req.body ||
        !Buffer.isBuffer(req.body)
    ) {
        return res
            .status(400)
            .send("OTA upload body missing");
    }

    let file;

    try {
        file = parseMultipartFile(
            req.body,
            req.headers["content-type"] || ""
        );
    } catch (error) {
        console.error(
            "[CLOUD OTA] Multipart parse error:",
            error.message
        );

        return res
            .status(400)
            .send(error.message);
    }

    const filename = String(
        file.filename || "firmware.bin"
    );

    if (
        !filename
            .toLowerCase()
            .endsWith(".bin")
    ) {
        return res
            .status(400)
            .send("Only .bin firmware files are accepted");
    }

    if (file.data.length === 0) {
        return res
            .status(400)
            .send("Firmware file is empty");
    }

    const id =
        `${Date.now()}-${Math.random()
            .toString(36)
            .slice(2, 10)}`;

    const apiKey = String(
        req.headers["x-api-key"] || ""
    );

    const timer = setTimeout(() => {
        const item = pending.get(id);
        if (!item) return;

        pending.delete(id);
        finishEspJob(id);

        if (!res.headersSent) {
            res
                .status(504)
                .send("ESP32 OTA timeout");
        }

        console.log(
            "[CLOUD OTA] Timeout:",
            id
        );
    }, 180000);

    pending.set(id, {
        res,
        timer,
        finished: false,
        statusCode: 200,
        headers: {},
        chunks: []
    });

    queueEspJob({
        id,
        kind: "ota",

        async send(ws) {
            console.log(
                "[CLOUD OTA] File:",
                filename,
                "bytes:",
                file.data.length
            );

            // Send the size as a STRING because the ESP32's lightweight
            // JSON parser reads quoted string values.
            await sendWs(ws, {
                type: "ota_start",
                id,
                filename,
                apiKey,
                totalBytes:
                    String(file.data.length)
            });

            // 512 raw bytes -> ~684 base64 characters + JSON envelope.
            // This stays comfortably below the WebSocket payload limit and
            // keeps the ESP32 memory usage small.
            const CHUNK_SIZE = 512;

            let sequence = 0;

            for (
                let offset = 0;
                offset < file.data.length;
                offset += CHUNK_SIZE
            ) {
                const chunk = file.data.slice(
                    offset,
                    Math.min(
                        offset + CHUNK_SIZE,
                        file.data.length
                    )
                );

                await sendWs(ws, {
                    type: "ota_chunk",
                    id,
                    seq: sequence++,
                    body: chunk.toString("base64")
                });
            }

            await sendWs(ws, {
                type: "ota_end",
                id,
                totalBytes:
                    String(file.data.length)
            });
        }
    });
}

// ============================================================
// ESP32 WEBSOCKET
// ============================================================

wss.on("connection", (ws) => {
    console.log("[WS] ESP32 socket connected");

    ws.isAlive = true;
    ws.lastHeartbeat = Date.now();

    ws.on("message", (message) => {
        try {
            const data = JSON.parse(message.toString());

            if (data.type !== "response_chunk") {
                console.log(
                    "[ESP32 -> RENDER]",
                    data.type
                );
            }

            // ------------------------------------------------
            // ESP32 registration
            // ------------------------------------------------

            if (data.type === "esp32") {
                // Replace an older connection safely. Do not let the old
                // socket's asynchronous "close" event cancel requests that
                // are now being handled by the new socket.
                if (esp32 && esp32 !== ws) {
                    console.log("[WS] Replacing old ESP32 connection");

                    if (activeEspJob) {
                        espQueue.unshift(activeEspJob);
                        activeEspJob = null;
                        espBusy = false;
                    }

                    try { esp32.close(); } catch {}
                }

                esp32 = ws;

                console.log(
                    "[WS] ESP32 registered"
                );

                ws.send(
                    JSON.stringify({
                        type: "registered"
                    })
                );

                pumpEspQueue();

                return;
            }

            // Ignore application messages from a superseded connection.
            if (esp32 !== ws) {
                return;
            }

            // ------------------------------------------------
            // Application heartbeat
            // ------------------------------------------------

            if (data.type === "heartbeat") {
                ws.isAlive = true;
                ws.lastHeartbeat = Date.now();

                try {
                    ws.send(JSON.stringify({
                        type: "heartbeat_ack"
                    }));
                } catch (err) {
                    console.error(
                        "[WS] Heartbeat ACK failed:",
                        err.message
                    );
                }

                return;
            }

            // Legacy response
            // ------------------------------------------------

            if (data.type === "response") {
                const item = pending.get(data.id);
                if (!item) return;

                let body;

                try {
                    body = Buffer.from(
                        data.body || "",
                        "base64"
                    );
                } catch {
                    failPending(
                        data.id,
                        502,
                        "Invalid response body from ESP32"
                    );
                    return;
                }

                item.statusCode =
                    Number(data.statusCode || 200);
                item.headers =
                    data.headers || {};

                sendPendingResponse(
                    data.id,
                    item,
                    body
                );

                return;
            }

            // ------------------------------------------------
            // Chunked response START
            // ------------------------------------------------

            if (data.type === "response_start") {
                const item = pending.get(data.id);
                if (!item) return;

                item.statusCode =
                    Number(data.statusCode || 200);
                item.headers =
                    data.headers || {};
                item.expectedLength =
                    Number.isFinite(Number(data.expectedLength))
                        ? Number(data.expectedLength)
                        : -1;
                item.chunks = [];
                item.receivedBytes = 0;
                item.finished = false;

                console.log(
                    "[TUNNEL] Response start",
                    item.statusCode,
                    "expected:",
                    item.expectedLength,
                    "id:",
                    data.id
                );

                return;
            }

            // ------------------------------------------------
            // Chunked response DATA
            // ------------------------------------------------

            if (data.type === "response_chunk") {
                const item = pending.get(data.id);
                if (!item || item.finished) return;

                let chunk;

                try {
                    chunk = Buffer.from(
                        data.body || "",
                        "base64"
                    );
                } catch {
                    failPending(
                        data.id,
                        502,
                        "Invalid response chunk from ESP32"
                    );
                    return;
                }

                if (chunk.length > 0) {
                    item.chunks.push(chunk);
                    item.receivedBytes += chunk.length;
                }

                return;
            }

            // ------------------------------------------------
            // Chunked response END
            // ------------------------------------------------

            if (data.type === "response_end") {
                const item = pending.get(data.id);
                if (!item || item.finished) return;

                const body = Buffer.concat(
                    item.chunks || []
                );

                console.log(
                    "[TUNNEL] Response end",
                    "received:", body.length,
                    "reported:", Number(data.totalBytes || 0),
                    "expected:", item.expectedLength,
                    "id:", data.id
                );

                sendPendingResponse(
                    data.id,
                    item,
                    body
                );

                return;
            }

        } catch (error) {
            console.error(
                "[WS] Message processing error:",
                error.message
            );
        }
    });

    ws.on("pong", () => {
        ws.isAlive = true;
    });

    ws.on("close", () => {
        console.log(
            "[WS] ESP32 socket closed"
        );

        // This can be an old socket that was intentionally replaced by a
        // newly registered ESP32. In that case, do nothing to the current
        // connection or its pending requests.
        if (esp32 !== ws) {
            return;
        }

        esp32 = null;

        // Keep queued/pending browser requests alive while ESP32 reconnects.
        // The request timers still provide an upper bound. The active job is
        // returned to the front so the new ESP32 connection can retry it.
        if (activeEspJob) {
            espQueue.unshift(activeEspJob);
            activeEspJob = null;
        }

        espBusy = false;

        console.log(
            "[WS] Waiting for ESP32 reconnect; queued requests preserved"
        );
    });

    ws.on("error", (error) => {
        console.error(
            "[WS] ESP32 socket error:",
            error.message
        );
    });
});

// ============================================================
// WEBSOCKET KEEPALIVE
// ============================================================

const heartbeatTimer = setInterval(() => {
    wss.clients.forEach((ws) => {
        if (ws.readyState !== WebSocket.OPEN) return;

        if (ws.isAlive === false) {
            console.log(
                "[WS] Terminating stale ESP32 connection"
            );

            try {
                ws.terminate();
            } catch {}

            return;
        }

        ws.isAlive = false;

        try {
            ws.ping();
        } catch (err) {
            console.error(
                "[WS] Ping failed:",
                err.message
            );
        }
    });
}, HEARTBEAT_INTERVAL);

wss.on("close", () => {
    clearInterval(heartbeatTimer);
});

// ============================================================
// VISITOR CAPACITY CONTROL
// ============================================================
// Maximum active browser visitors through the public Render URL.
// The ESP32/admin source code does not need any visitor-count code.
//
// A visitor is identified by a random cookie. Multiple tabs in the same
// browser share the same slot. Activity on any proxied request refreshes
// the visitor's lastSeen time. When a browser disappears, its slot is
// released automatically after VISITOR_IDLE_MS.
// ============================================================

const MAX_ACTIVE_VISITORS = 100;
const VISITOR_IDLE_MS = 90 * 1000;
const VISITOR_COOKIE = "relayhub_visitor";
const VISITOR_COOKIE_MAX_AGE = 60 * 60;

const activeVisitors = new Map();

let waitingVisitors = 0;

function createVisitorId() {
    if (typeof crypto !== "undefined" && crypto.randomUUID) {
        return crypto.randomUUID();
    }

    return `${Date.now()}-${Math.random().toString(36).slice(2, 14)}-${Math.random().toString(36).slice(2, 14)}`;
}

function parseCookieHeader(cookieHeader) {
    const result = {};

    String(cookieHeader || "")
        .split(";")
        .forEach((part) => {
            const index = part.indexOf("=");

            if (index <= 0) {
                return;
            }

            const name =
                part.slice(0, index).trim();

            const value =
                part.slice(index + 1).trim();

            if (name) {
                result[name] = value;
            }
        });

    return result;
}

function getVisitorId(req) {
    const cookies =
        parseCookieHeader(
            req.headers.cookie || ""
        );

    return (
        cookies[VISITOR_COOKIE] ||
        ""
    );
}

function setVisitorCookie(
    res,
    visitorId
) {
    res.setHeader(
        "Set-Cookie",
        `${VISITOR_COOKIE}=${visitorId}; Path=/; Max-Age=${VISITOR_COOKIE_MAX_AGE}; SameSite=Lax; Secure`
    );
}

function cleanupVisitors() {
    const now = Date.now();

    for (
        const [
            visitorId,
            visitor
        ] of activeVisitors.entries()
    ) {
        if (
            now - visitor.lastSeen >
            VISITOR_IDLE_MS
        ) {
            activeVisitors.delete(
                visitorId
            );
        }
    }
}

function getVisitorStats() {
    cleanupVisitors();

    return {
        max: MAX_ACTIVE_VISITORS,

        active:
            activeVisitors.size,

        available:
            Math.max(
                0,
                MAX_ACTIVE_VISITORS -
                    activeVisitors.size
            ),

        waiting:
            waitingVisitors
    };
}

function visitorWaitPage(
    stats
) {
    const active =
        Number(
            stats.active || 0
        );

    const max =
        Number(
            stats.max ||
                MAX_ACTIVE_VISITORS
        );

    const waiting =
        Number(
            stats.waiting || 0
        );

    return `<!doctype html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Server Busy - Please Wait</title>
<style>
body{margin:0;font-family:Arial,sans-serif;background:#0f172a;color:#e5e7eb;display:flex;align-items:center;justify-content:center;min-height:100vh}
.card{width:min(460px,calc(100% - 32px));background:#1e293b;border-radius:18px;padding:30px;box-sizing:border-box;text-align:center;box-shadow:0 20px 60px rgba(0,0,0,.35)}
.spinner{width:44px;height:44px;border:4px solid #475569;border-top-color:#e5e7eb;border-radius:50%;animation:spin 1s linear infinite;margin:0 auto 20px}
@keyframes spin{to{transform:rotate(360deg)}}
h1{font-size:24px;margin:0 0 12px}
p{color:#cbd5e1;line-height:1.5}
.stats{margin:20px 0;padding:14px;border-radius:12px;background:#0f172a}
.small{font-size:13px;color:#94a3b8}
button{margin-top:14px;padding:10px 18px;border:0;border-radius:10px;cursor:pointer;font-size:15px}
</style>
</head>
<body>
<div class="card">
<div class="spinner"></div>
<h1>All visitor slots are busy</h1>
<p>Your request is waiting for an available visitor slot.</p>
<div class="stats">
<strong>${active} / ${max}</strong> active visitors<br>
<span class="small">Waiting: ${waiting}</span>
</div>
<p class="small">This page will retry automatically.</p>
<button onclick="location.reload()">Retry now</button>
</div>
<script>
setTimeout(() => location.reload(), 5000);
</script>
</body>
</html>`;
}

// Internal visitor endpoints are not counted as new visitors.
app.get(
    "/__visitor/pulse",
    (req, res) => {
        const visitorId =
            getVisitorId(req);

        if (
            visitorId &&
            activeVisitors.has(
                visitorId
            )
        ) {
            activeVisitors.get(
                visitorId
            ).lastSeen =
                Date.now();
        }

        res.json(
            getVisitorStats()
        );
    }
);

app.get(
    "/__visitor/stats",
    (req, res) => {
        res.json(
            getVisitorStats()
        );
    }
);

app.post(
    "/__visitor/leave",
    (req, res) => {
        const visitorId =
            getVisitorId(req);

        if (visitorId) {
            activeVisitors.delete(
                visitorId
            );
        }

        res.status(204).end();
    }
);

// Capacity gate. It is implemented only at the public Render gateway.
// No change to the ESP32 /admin page is required.
app.use(
    (req, res, next) => {
        if (
            req.path.startsWith(
                "/__visitor/"
            ) ||
            req.path === "/health" ||
            req.path ===
                "/cloud-status"
        ) {
            return next();
        }

        cleanupVisitors();

        let visitorId =
            getVisitorId(req);

        if (
            visitorId &&
            activeVisitors.has(
                visitorId
            )
        ) {
            activeVisitors.get(
                visitorId
            ).lastSeen =
                Date.now();

            res.setHeader(
                "X-RelayHub-Visitors",
                `${activeVisitors.size}/${MAX_ACTIVE_VISITORS}`
            );

            return next();
        }

        if (!visitorId) {
            visitorId =
                createVisitorId();
        }

        if (
            activeVisitors.size >=
            MAX_ACTIVE_VISITORS
        ) {
            waitingVisitors++;

            const stats =
                getVisitorStats();

            const html =
                visitorWaitPage(
                    stats
                );

            setVisitorCookie(
                res,
                visitorId
            );

            res.setHeader(
                "Retry-After",
                "5"
            );

            res.setHeader(
                "X-RelayHub-Visitors",
                `${stats.active}/${stats.max}`
            );

            waitingVisitors =
                Math.max(
                    0,
                    waitingVisitors - 1
                );

            return res
                .status(429)
                .type("html")
                .send(html);
        }

        activeVisitors.set(
            visitorId,
            {
                connectedAt:
                    Date.now(),

                lastSeen:
                    Date.now()
            }
        );

        setVisitorCookie(
            res,
            visitorId
        );

        res.setHeader(
            "X-RelayHub-Visitors",
            `${activeVisitors.size}/${MAX_ACTIVE_VISITORS}`
        );

        return next();
    }
);

setInterval(
    () => {
        cleanupVisitors();
    },
    15000
);

// ============================================================
// STATUS
// ============================================================

app.get(
    "/cloud-status",
    (req, res) => {
        res.json({
            render: true,

            esp32Connected:
                !!esp32 &&
                esp32.readyState ===
                    WebSocket.OPEN,

            pendingRequests:
                pending.size
        });
    }
);

app.get(
    "/health",
    (req, res) => {
        res.json({
            ok: true,

            esp32Connected:
                !!esp32 &&
                esp32.readyState ===
                    WebSocket.OPEN
        });
    }
);

// ============================================================
// RAW BODY
// ============================================================

app.use(
    express.raw({
        type: "*/*",
        limit: "10mb"
    })
);

// ============================================================
// EXISTING CLOUD UPLOAD
// Must be checked before the normal HTTP -> ESP32 proxy.
// ============================================================

app.post(
    "/upload",
    (req, res) => {
        handleCloudUpload(
            req,
            res
        ).catch(
            (error) => {
                console.error(
                    "[CLOUD UPLOAD] Unexpected error:",
                    error
                );

                if (
                    !res.headersSent
                ) {
                    res
                        .status(500)
                        .send(
                            "Upload failed"
                        );
                }
            }
        );
    }
);

// ============================================================
// CLOUD OTA ROUTE
// Must be checked before the normal HTTP -> ESP32 proxy.
// ============================================================

app.post(
    "/api/ota",
    (req, res) => {
        handleCloudOTA(
            req,
            res
        ).catch(
            (error) => {
                console.error(
                    "[CLOUD OTA] Unexpected error:",
                    error
                );

                if (
                    !res.headersSent
                ) {
                    res
                        .status(500)
                        .send(
                            "OTA upload failed"
                        );
                }
            }
        );
    }
);

// ============================================================
// NORMAL HTTP -> ESP32 PROXY
// ============================================================

app.use(
    (req, res) => {
        console.log(
            "[HTTP -> ESP32]",
            req.method,
            req.originalUrl
        );

        if (
            !esp32 ||
            esp32.readyState !==
                WebSocket.OPEN
        ) {
            return res
                .status(503)
                .send(
                    "ESP32 is not connected"
                );
        }

        const id =
            `${Date.now()}-${Math.random()
                .toString(36)
                .slice(2, 10)}`;

        const headers = {};

        for (
            const [
                key,
                value
            ] of Object.entries(
                req.headers
            )
        ) {
            const lower =
                key.toLowerCase();

            if (
                lower ===
                    "cookie" ||
                lower ===
                    "content-type" ||
                lower ===
                    "content-length" ||
                lower ===
                    "x-api-key" ||
                lower ===
                    "authorization" ||
                lower ===
                    "user-agent" ||
                lower ===
                    "accept" ||
                lower ===
                    "accept-language" ||
                lower ===
                    "referer" ||
                lower ===
                    "origin"
            ) {
                headers[lower] =
                    Array.isArray(
                        value
                    )
                        ? value.join(
                              ", "
                          )
                        : String(
                              value
                          );
            }
        }

        let body = "";

        if (
            req.body &&
            Buffer.isBuffer(
                req.body
            ) &&
            req.body.length > 0
        ) {
            body =
                req.body.toString(
                    "base64"
                );
        }

        const timer =
            setTimeout(
                () => {
                    const item =
                        pending.get(
                            id
                        );

                    if (!item) {
                        return;
                    }

                    pending.delete(
                        id
                    );

                    finishEspJob(
                        id
                    );

                    if (
                        !res.headersSent
                    ) {
                        res
                            .status(504)
                            .send(
                                "ESP32 request timeout"
                            );
                    }
                },
                30000
            );

        pending.set(
            id,
            {
                res,
                timer,
                finished: false,
                chunks: [],
                receivedBytes: 0,
                statusCode: 200,
                headers: {}
            }
        );

        const message = {
            type: "request",
            id,
            method:
                req.method,
            path:
                req.originalUrl,
            headers,
            body
        };

        queueEspJob({
            id,
            kind: "request",

            async send(ws) {
                await sendWs(
                    ws,
                    message
                );

                console.log(
                    "[WS] Request sent:",
                    req.method,
                    req.originalUrl,
                    "id:",
                    id
                );
            }
        });
    }
);

// ============================================================
// SERVER
// ============================================================

const PORT =
    process.env.PORT || 3000;

server.listen(
    PORT,
    () => {
        console.log(
            "========================================"
        );

        console.log(
            `Render gateway listening on port ${PORT}`
        );

        console.log(
            "WebSocket endpoint: /tunnel"
        );

        console.log(
            "Cloud upload endpoint: POST /upload"
        );

        console.log(
            "Cloud OTA endpoint: POST /api/ota"
        );

        console.log(
            "Maximum active visitors:",
            MAX_ACTIVE_VISITORS
        );

        console.log(
            "========================================"
        );
    }
);

process.on(
    "uncaughtException",
    (error) => {
        console.error(
            "[PROCESS] Uncaught exception:",
            error
        );
    }
);

process.on(
    "unhandledRejection",
    (error) => {
        console.error(
            "[PROCESS] Unhandled rejection:",
            error
        );
    }
);
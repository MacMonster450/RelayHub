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
        try {
            item.res.end();
        } catch {}
    }

    finishEspJob(id);
}

function queueEspJob(job) {
    espQueue.push(job);
    pumpEspQueue();
}

async function pumpEspQueue() {
    if (espBusy) {
        return;
    }

    if (
        !esp32 ||
        esp32.readyState !== WebSocket.OPEN
    ) {
        return;
    }

    while (espQueue.length > 0) {
        const job = espQueue.shift();

        if (!job) {
            return;
        }

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
        if (
            activeEspJob &&
            activeEspJob.id === job.id
        ) {
            return;
        }

        if (
            !esp32 ||
            esp32.readyState !== WebSocket.OPEN
        ) {
            return;
        }

        if (espBusy) {
            return;
        }
    }
}

function sendPendingResponse(id, item, body) {
    if (!item || item.finished) {
        return;
    }

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

        if (
            value === undefined ||
            value === null
        ) {
            continue;
        }

        if (lower === "set-cookie") {
            item.res.setHeader(
                "Set-Cookie",
                Array.isArray(value)
                    ? value.map(String)
                    : [String(value)]
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

    item.res.setHeader(
        "Content-Length",
        body.length
    );

    item.res.setHeader(
        "Cache-Control",
        headers["cache-control"] || "no-store"
    );

    const statusCode =
        Number(item.statusCode || 200);

    console.log(
        "[HTTP] -> Browser",
        statusCode,
        "bytes:",
        body.length,
        "id:",
        id
    );

    item.res
        .status(statusCode)
        .end(body);

    finishEspJob(id);
}

function sendWs(ws, payload) {
    return new Promise((resolve, reject) => {
        if (
            !ws ||
            ws.readyState !== WebSocket.OPEN
        ) {
            reject(
                new Error(
                    "ESP32 WebSocket is not connected"
                )
            );

            return;
        }

        ws.send(
            JSON.stringify(payload),
            (err) => {
                if (err) {
                    reject(err);
                } else {
                    resolve();
                }
            }
        );
    });
}

// ------------------------------------------------------------
// Multipart parser for the existing /upload form.
// The ESP32's normal local /upload route is untouched.
// Cloud uploads use a dedicated WebSocket file-transfer path.
// ------------------------------------------------------------

function parseMultipartFile(
    body,
    contentType
) {
    if (!Buffer.isBuffer(body)) {
        throw new Error(
            "Upload body is not a Buffer"
        );
    }

    const match =
        /boundary=(?:"([^"]+)"|([^;]+))/i.exec(
            contentType || ""
        );

    if (!match) {
        throw new Error(
            "Multipart boundary not found"
        );
    }

    const boundary =
        match[1] || match[2];

    const marker =
        Buffer.from(
            `--${boundary}`,
            "utf8"
        );

    let cursor =
        body.indexOf(marker);

    while (cursor >= 0) {
        const partStart =
            cursor + marker.length;

        // End of multipart body.
        if (
            body
                .slice(
                    partStart,
                    partStart + 2
                )
                .toString() === "--"
        ) {
            break;
        }

        let headerStart =
            partStart;

        if (
            body[headerStart] === 0x0d &&
            body[headerStart + 1] === 0x0a
        ) {
            headerStart += 2;
        }

        const headerEnd =
            body.indexOf(
                Buffer.from(
                    "\r\n\r\n"
                ),
                headerStart
            );

        if (headerEnd < 0) {
            throw new Error(
                "Incomplete multipart headers"
            );
        }

        const headersText =
            body
                .slice(
                    headerStart,
                    headerEnd
                )
                .toString("utf8");

        const disposition =
            /content-disposition:\s*([^\r\n]+)/i.exec(
                headersText
            );

        if (disposition) {
            const value =
                disposition[1];

            const filenameMatch =
                /filename="([^"]*)"/i.exec(
                    value
                );

            if (filenameMatch) {
                const filename =
                    filenameMatch[1];

                const dataStart =
                    headerEnd + 4;

                const nextBoundary =
                    body.indexOf(
                        Buffer.from(
                            `\r\n--${boundary}`
                        ),
                        dataStart
                    );

                if (nextBoundary < 0) {
                    throw new Error(
                        "Multipart closing boundary not found"
                    );
                }

                const fileData =
                    body.slice(
                        dataStart,
                        nextBoundary
                    );

                return {
                    filename,
                    data: fileData
                };
            }
        }

        cursor =
            body.indexOf(
                marker,
                partStart
            );
    }

    throw new Error(
        "No file found in multipart upload"
    );
}

async function handleCloudUpload(
    req,
    res
) {
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
            .send(
                "ESP32 is not connected"
            );
    }

    if (
        !req.body ||
        !Buffer.isBuffer(req.body)
    ) {
        return res
            .status(400)
            .send(
                "Upload body missing"
            );
    }

    let file;

    try {
        file =
            parseMultipartFile(
                req.body,
                req.headers["content-type"] ||
                    ""
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
        String(
            req.headers.cookie || ""
        );

    const timer =
        setTimeout(() => {
            const item =
                pending.get(id);

            if (!item) {
                return;
            }

            pending.delete(id);

            finishEspJob(id);

            if (!res.headersSent) {
                res
                    .status(504)
                    .send(
                        "ESP32 upload timeout"
                    );
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

            await sendWs(
                ws,
                {
                    type: "upload_start",
                    id,
                    filename:
                        file.filename,
                    cookie
                }
            );

            // Keep chunks small enough
            // for ESP32 WebSocket memory.
            const CHUNK_SIZE = 768;

            let sequence = 0;

            for (
                let offset = 0;
                offset < file.data.length;
                offset += CHUNK_SIZE
            ) {
                const chunk =
                    file.data.slice(
                        offset,
                        Math.min(
                            offset +
                                CHUNK_SIZE,
                            file.data.length
                        )
                    );

                await sendWs(
                    ws,
                    {
                        type:
                            "upload_chunk",
                        id,
                        seq:
                            sequence++,
                        body:
                            chunk.toString(
                                "base64"
                            )
                    }
                );
            }

            await sendWs(
                ws,
                {
                    type: "upload_end",
                    id,
                    totalBytes:
                        file.data.length
                }
            );
        }
    });
}

// ============================================================
// ESP32 WEBSOCKET
// ============================================================

wss.on(
    "connection",
    (ws) => {
        console.log(
            "[WS] ESP32 socket connected"
        );

        ws.isAlive = true;
        ws.lastHeartbeat = Date.now();

        ws.on(
            "message",
            (message) => {
                try {
                    const data =
                        JSON.parse(
                            message.toString()
                        );

                    if (
                        data.type !==
                        "response_chunk"
                    ) {
                        console.log(
                            "[ESP32 -> RENDER]",
                            data.type
                        );
                    }

                    // ------------------------------------------------
                    // ESP32 registration
                    // ------------------------------------------------

                    if (
                        data.type === "esp32"
                    ) {
                        // Replace an older connection safely.
                        // Do not let the old socket's asynchronous
                        // "close" event cancel requests that are now
                        // being handled by the new socket.
                        if (
                            esp32 &&
                            esp32 !== ws
                        ) {
                            console.log(
                                "[WS] Replacing old ESP32 connection"
                            );

                            if (
                                activeEspJob
                            ) {
                                espQueue.unshift(
                                    activeEspJob
                                );

                                activeEspJob =
                                    null;

                                espBusy =
                                    false;
                            }

                            try {
                                esp32.close();
                            } catch {}
                        }

                        esp32 = ws;

                        console.log(
                            "[WS] ESP32 registered"
                        );

                        ws.send(
                            JSON.stringify({
                                type:
                                    "registered"
                            })
                        );

                        pumpEspQueue();

                        return;
                    }

                    // Ignore application messages
                    // from a superseded connection.
                    if (
                        esp32 !== ws
                    ) {
                        return;
                    }

                    // ------------------------------------------------
                    // Application heartbeat
                    // ------------------------------------------------

                    if (
                        data.type ===
                        "heartbeat"
                    ) {
                        ws.isAlive =
                            true;

                        ws.lastHeartbeat =
                            Date.now();

                        try {
                            ws.send(
                                JSON.stringify({
                                    type:
                                        "heartbeat_ack"
                                })
                            );
                        } catch (
                            err
                        ) {
                            console.error(
                                "[WS] Heartbeat ACK failed:",
                                err.message
                            );
                        }

                        return;
                    }

                    // ------------------------------------------------
                    // Legacy response
                    // ------------------------------------------------

                    if (
                        data.type ===
                        "response"
                    ) {
                        const item =
                            pending.get(
                                data.id
                            );

                        if (!item) {
                            return;
                        }

                        let body;

                        try {
                            body =
                                Buffer.from(
                                    data.body ||
                                        "",
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
                            Number(
                                data.statusCode ||
                                    200
                            );

                        item.headers =
                            data.headers ||
                            {};

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

                    if (
                        data.type ===
                        "response_start"
                    ) {
                        const item =
                            pending.get(
                                data.id
                            );

                        if (!item) {
                            return;
                        }

                        item.statusCode =
                            Number(
                                data.statusCode ||
                                    200
                            );

                        item.headers =
                            data.headers ||
                            {};

                        item.expectedLength =
                            Number.isFinite(
                                Number(
                                    data.expectedLength
                                )
                            )
                                ? Number(
                                      data.expectedLength
                                  )
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

                    if (
                        data.type ===
                        "response_chunk"
                    ) {
                        const item =
                            pending.get(
                                data.id
                            );

                        if (
                            !item ||
                            item.finished
                        ) {
                            return;
                        }

                        let chunk;

                        try {
                            chunk =
                                Buffer.from(
                                    data.body ||
                                        "",
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

                        if (
                            chunk.length >
                            0
                        ) {
                            item.chunks.push(
                                chunk
                            );

                            item.receivedBytes +=
                                chunk.length;
                        }

                        return;
                    }

                    // ------------------------------------------------
                    // Chunked response END
                    // ------------------------------------------------

                    if (
                        data.type ===
                        "response_end"
                    ) {
                        const item =
                            pending.get(
                                data.id
                            );

                        if (
                            !item ||
                            item.finished
                        ) {
                            return;
                        }

                        const body =
                            Buffer.concat(
                                item.chunks ||
                                    []
                            );

                        console.log(
                            "[TUNNEL] Response end",
                            "received:",
                            body.length,
                            "reported:",
                            Number(
                                data.totalBytes ||
                                    0
                            ),
                            "expected:",
                            item.expectedLength,
                            "id:",
                            data.id
                        );

                        sendPendingResponse(
                            data.id,
                            item,
                            body
                        );

                        return;
                    }
                } catch (
                    error
                ) {
                    console.error(
                        "[WS] Message processing error:",
                        error.message
                    );
                }
            }
        );

        ws.on(
            "pong",
            () => {
                ws.isAlive = true;
            }
        );

        ws.on(
            "close",
            () => {
                console.log(
                    "[WS] ESP32 socket closed"
                );

                // This can be an old socket that was intentionally
                // replaced by a newly registered ESP32. In that case,
                // do nothing to the current connection or its
                // pending requests.
                if (
                    esp32 !== ws
                ) {
                    return;
                }

                esp32 = null;

                // Keep queued/pending browser requests alive while
                // ESP32 reconnects.
                //
                // The request timers still provide an upper bound.
                // The active job is returned to the front so the new
                // ESP32 connection can retry it.
                if (
                    activeEspJob
                ) {
                    espQueue.unshift(
                        activeEspJob
                    );

                    activeEspJob =
                        null;
                }

                espBusy = false;

                console.log(
                    "[WS] Waiting for ESP32 reconnect; queued requests preserved"
                );

                pumpEspQueue();
            }
        );

        ws.on(
            "error",
            (error) => {
                console.error(
                    "[WS] ESP32 socket error:",
                    error.message
                );
            }
        );
    }
);

// ============================================================
// WEBSOCKET KEEPALIVE
// ============================================================

const heartbeatTimer =
    setInterval(
        () => {
            wss.clients.forEach(
                (ws) => {
                    if (
                        ws.readyState !==
                        WebSocket.OPEN
                    ) {
                        return;
                    }

                    if (
                        ws.isAlive ===
                        false
                    ) {
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
                    } catch (
                        err
                    ) {
                        console.error(
                            "[WS] Ping failed:",
                            err.message
                        );
                    }
                }
            );
        },
        HEARTBEAT_INTERVAL
    );

wss.on(
    "close",
    () => {
        clearInterval(
            heartbeatTimer
        );
    }
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
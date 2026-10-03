const express = require("express");
const http = require("http");
const WebSocket = require("ws");

const app = express();
const server = http.createServer(app);

const wss = new WebSocket.Server({
    server,
    path: "/tunnel"
});

let esp32 = null;
const pending = new Map();

function failPending(id, status, message) {
    const item = pending.get(id);
    if (!item) return;

    clearTimeout(item.timer);
    pending.delete(id);

    if (!item.res.headersSent) {
        item.res.status(status).send(message);
    } else {
        try {
            item.res.end();
        } catch {}
    }
}

function sendPendingResponse(id, item, body) {
    if (item.finished) return;

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

    // Use the actual number of bytes received.
    item.res.setHeader(
        "Content-Length",
        body.length
    );

    item.res.setHeader(
        "Cache-Control",
        headers["cache-control"] || "no-store"
    );

    console.log(
        "[HTTP] -> Browser",
        item.statusCode || 200,
        "bytes:",
        body.length,
        "id:",
        id
    );

    item.res.status(
        Number(item.statusCode || 200)
    );

    item.res.end(body);
}

// ============================================================
// ESP32 WEBSOCKET CONNECTION
// ============================================================

wss.on("connection", (ws) => {
    console.log("[WS] ESP32 socket connected");

    ws.on("message", (message) => {
        try {
            const data = JSON.parse(
                message.toString()
            );

            if (data.type !== "response_chunk") {
                console.log(
                    "[ESP32 -> RENDER]",
                    data.type
                );
            }

            // ========================================================
            // ESP32 REGISTRATION
            // ========================================================

            if (data.type === "esp32") {
                if (esp32 && esp32 !== ws) {
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
                        type: "registered"
                    })
                );

                return;
            }

            // ========================================================
            // LEGACY SINGLE RESPONSE SUPPORT
            // ========================================================

            if (data.type === "response") {
                const item = pending.get(
                    data.id
                );

                if (!item) {
                    console.log(
                        "[WS] Unknown response ID:",
                        data.id
                    );
                    return;
                }

                let body;

                try {
                    body = Buffer.from(
                        data.body || "",
                        "base64"
                    );
                } catch (err) {
                    failPending(
                        data.id,
                        502,
                        "Invalid response body from ESP32"
                    );
                    return;
                }

                item.statusCode =
                    Number(
                        data.statusCode || 200
                    );

                item.headers =
                    data.headers || {};

                sendPendingResponse(
                    data.id,
                    item,
                    body
                );

                return;
            }

            // ========================================================
            // CHUNKED RESPONSE START
            // ========================================================

            if (
                data.type ===
                "response_start"
            ) {
                const item =
                    pending.get(data.id);

                if (!item) {
                    console.log(
                        "[WS] Unknown response_start ID:",
                        data.id
                    );
                    return;
                }

                item.statusCode =
                    Number(
                        data.statusCode || 200
                    );

                item.headers =
                    data.headers || {};

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

            // ========================================================
            // CHUNKED RESPONSE DATA
            // ========================================================

            if (
                data.type ===
                "response_chunk"
            ) {
                const item =
                    pending.get(data.id);

                if (!item || item.finished) {
                    console.log(
                        "[WS] Unknown response_chunk ID:",
                        data.id
                    );
                    return;
                }

                let chunk;

                try {
                    chunk = Buffer.from(
                        data.body || "",
                        "base64"
                    );
                } catch (err) {
                    failPending(
                        data.id,
                        502,
                        "Invalid response chunk from ESP32"
                    );
                    return;
                }

                if (chunk.length > 0) {
                    item.chunks.push(chunk);

                    item.receivedBytes +=
                        chunk.length;
                }

                return;
            }

            // ========================================================
            // CHUNKED RESPONSE END
            // ========================================================

            if (
                data.type ===
                "response_end"
            ) {
                const item =
                    pending.get(data.id);

                if (!item || item.finished) {
                    console.log(
                        "[WS] Unknown response_end ID:",
                        data.id
                    );
                    return;
                }

                const body = Buffer.concat(
                    item.chunks || []
                );

                const reportedBytes =
                    Number(
                        data.totalBytes
                    );

                console.log(
                    "[TUNNEL] Response end",
                    "received:",
                    body.length,
                    "reported:",
                    reportedBytes,
                    "expected:",
                    item.expectedLength,
                    "id:",
                    data.id
                );

                // Do not blindly trust Content-Length.
                // Forward exactly what was received.
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

    // ========================================================
    // ESP32 DISCONNECTED
    // ========================================================

    ws.on("close", () => {
        console.log(
            "[WS] ESP32 socket closed"
        );

        if (esp32 === ws) {
            esp32 = null;
        }

        for (
            const [id, item]
            of pending.entries()
        ) {
            clearTimeout(
                item.timer
            );

            if (!item.res.headersSent) {
                item.res
                    .status(503)
                    .send(
                        "ESP32 disconnected"
                    );
            }

            pending.delete(id);
        }
    });

    // ========================================================
    // WEBSOCKET ERROR
    // ========================================================

    ws.on("error", (error) => {
        console.error(
            "[WS] ESP32 socket error:",
            error.message
        );
    });
});

// ============================================================
// CLOUD STATUS
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

// ============================================================
// HEALTH
// ============================================================

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
// RAW BODY PARSER
// ============================================================

app.use(
    express.raw({
        type: "*/*",
        limit: "10mb"
    })
);

// ============================================================
// HTTP -> ESP32
// ============================================================

app.use((req, res) => {
    console.log(
        "[HTTP -> ESP32]",
        req.method,
        req.originalUrl
    );

    // ========================================================
    // CHECK ESP32 CONNECTION
    // ========================================================

    if (
        !esp32 ||
        esp32.readyState !==
            WebSocket.OPEN
    ) {
        console.log(
            "[HTTP] ESP32 not connected"
        );

        return res
            .status(503)
            .send(
                "ESP32 is not connected"
            );
    }

    // ========================================================
    // REQUEST ID
    // ========================================================

    const id =
        `${Date.now()}-${Math.random()
            .toString(36)
            .slice(2, 10)}`;

    // ========================================================
    // REQUEST HEADERS
    // ========================================================

    const headers = {};

    for (
        const [key, value]
        of Object.entries(req.headers)
    ) {
        const lower =
            key.toLowerCase();

        if (
            lower === "cookie" ||
            lower === "content-type" ||
            lower === "content-length" ||
            lower === "x-api-key" ||
            lower === "authorization" ||
            lower === "user-agent" ||
            lower === "accept" ||
            lower === "accept-language" ||
            lower === "referer" ||
            lower === "origin"
        ) {
            headers[lower] =
                Array.isArray(value)
                    ? value.join(", ")
                    : String(value);
        }
    }

    // ========================================================
    // REQUEST BODY -> BASE64
    // ========================================================

    let body = "";

    if (
        req.body &&
        Buffer.isBuffer(req.body) &&
        req.body.length > 0
    ) {
        body =
            req.body.toString("base64");
    }

    // ========================================================
    // TIMEOUT
    // ========================================================

    const timer = setTimeout(
        () => {
            const item =
                pending.get(id);

            if (!item) {
                return;
            }

            pending.delete(id);

            console.log(
                "[HTTP] Request timeout:",
                req.method,
                req.originalUrl,
                id
            );

            if (!res.headersSent) {
                res
                    .status(504)
                    .send(
                        "ESP32 request timeout"
                    );
            }
        },
        30000
    );

    // ========================================================
    // STORE PENDING REQUEST
    // ========================================================

    pending.set(id, {
        res,
        timer,

        chunks: [],

        receivedBytes: 0,

        finished: false,

        statusCode: 200,

        headers: {}
    });

    // ========================================================
    // REQUEST MESSAGE
    // ========================================================

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

    // ========================================================
    // SEND TO ESP32
    // ========================================================

    try {
        esp32.send(
            JSON.stringify(message)
        );

        console.log(
            "[WS] Request sent:",
            req.method,
            req.originalUrl,
            "id:",
            id
        );

    } catch (error) {
        clearTimeout(timer);

        pending.delete(id);

        console.error(
            "[WS] Failed to send request:",
            error.message
        );

        if (!res.headersSent) {
            res
                .status(502)
                .send(
                    "Failed to send request to ESP32"
                );
        }
    }
});

// ============================================================
// PORT
// ============================================================

const PORT =
    process.env.PORT || 3000;

// ============================================================
// START SERVER
// ============================================================

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
            "========================================"
        );
    }
);

// ============================================================
// PROCESS ERROR HANDLING
// ============================================================

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
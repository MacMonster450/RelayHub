const express = require("express");
const http = require("http");
const WebSocket = require("ws");

const app = express();
const server = http.createServer(app);

// ============================================================
// WEBSOCKET SERVER FOR ESP32
// ============================================================

const wss = new WebSocket.Server({
    server,
    path: "/tunnel"
});

let esp32 = null;

// Browser requests waiting for ESP32 response
const pending = new Map();

// ============================================================
// WEBSOCKET CONNECTION
// ============================================================

wss.on("connection", (ws) => {
    console.log("[WS] ESP32 socket connected");

    ws.on("message", (message) => {
        try {
            const data = JSON.parse(message.toString());

            console.log("[ESP32 -> RENDER]", data.type);

            // =================================================
            // ESP32 REGISTRATION
            // =================================================

            if (data.type === "esp32") {

                // Close previous ESP32 connection
                if (esp32 && esp32 !== ws) {
                    try {
                        esp32.close();
                    } catch (err) {
                        console.error(
                            "[WS] Failed to close old connection:",
                            err.message
                        );
                    }
                }

                esp32 = ws;

                console.log("[WS] ESP32 registered");

                ws.send(
                    JSON.stringify({
                        type: "registered"
                    })
                );

                return;
            }

            // =================================================
            // RESPONSE FROM ESP32
            // =================================================

            if (data.type === "response") {

                const item = pending.get(data.id);

                if (!item) {
                    console.log(
                        "[WS] Unknown response ID:",
                        data.id
                    );
                    return;
                }

                clearTimeout(item.timer);
                pending.delete(data.id);

                // ------------------------------------------------
                // DECODE BASE64 BODY
                // ------------------------------------------------

                let body;

                try {
                    body = Buffer.from(
                        data.body || "",
                        "base64"
                    );
                } catch (err) {

                    console.error(
                        "[HTTP] Base64 decode error:",
                        err.message
                    );

                    if (!item.res.headersSent) {
                        item.res
                            .status(502)
                            .send(
                                "Invalid response body from ESP32"
                            );
                    }

                    return;
                }

                const headers = data.headers || {};

                // ------------------------------------------------
                // FORWARD RESPONSE HEADERS
                // ------------------------------------------------

                for (
                    const [key, value]
                    of Object.entries(headers)
                ) {

                    const lower = key.toLowerCase();

                    // Render controls these headers
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

                    // ------------------------------------------------
                    // COOKIE
                    // ------------------------------------------------

                    if (lower === "set-cookie") {

                        if (Array.isArray(value)) {

                            item.res.setHeader(
                                "Set-Cookie",
                                value.map(String)
                            );

                        } else {

                            item.res.setHeader(
                                "Set-Cookie",
                                [String(value)]
                            );
                        }

                        continue;
                    }

                    // ------------------------------------------------
                    // NORMAL HEADERS
                    // ------------------------------------------------

                    if (Array.isArray(value)) {

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

                // ------------------------------------------------
                // IMPORTANT:
                // SET EXACT BODY LENGTH
                // ------------------------------------------------

                item.res.setHeader(
                    "Content-Length",
                    body.length
                );

                // ------------------------------------------------
                // CACHE CONTROL
                // ------------------------------------------------

                if (
                    !item.res.getHeader(
                        "Cache-Control"
                    )
                ) {
                    item.res.setHeader(
                        "Cache-Control",
                        headers["cache-control"] ||
                        "no-store"
                    );
                }

                // ------------------------------------------------
                // SEND RESPONSE TO BROWSER
                // ------------------------------------------------

                const statusCode =
                    Number(data.statusCode || 200);

                console.log(
                    "[HTTP] -> Browser",
                    statusCode,
                    "bytes:",
                    body.length,
                    "request:",
                    data.id
                );

                if (!item.res.headersSent) {
                    item.res.status(statusCode);
                }

                item.res.end(body);

                return;
            }

        } catch (error) {

            console.error(
                "[WS] Message processing error:",
                error.message
            );
        }
    });

    // =========================================================
    // ESP32 CLOSED
    // =========================================================

    ws.on("close", () => {

        console.log(
            "[WS] ESP32 socket closed"
        );

        if (esp32 === ws) {
            esp32 = null;
        }

        // Fail all waiting browser requests
        for (
            const [id, item]
            of pending.entries()
        ) {

            clearTimeout(item.timer);

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

    // =========================================================
    // ESP32 ERROR
    // =========================================================

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

app.get("/cloud-status", (req, res) => {

    res.json({
        render: true,

        esp32Connected:
            !!esp32 &&
            esp32.readyState === WebSocket.OPEN,

        pendingRequests:
            pending.size
    });
});

// ============================================================
// HEALTH CHECK
// ============================================================

app.get("/health", (req, res) => {

    res.json({
        ok: true,

        esp32Connected:
            !!esp32 &&
            esp32.readyState === WebSocket.OPEN
    });
});

// ============================================================
// RAW REQUEST BODY
// ============================================================
//
// Supports:
//
// POST /login
// POST /upload
// POST /api/files
// POST /api/data
// POST /api/ota
//
// ============================================================

app.use(
    express.raw({
        type: "*/*",
        limit: "10mb"
    })
);

// ============================================================
// HTTP REQUEST -> ESP32
// ============================================================

app.use((req, res) => {

    console.log(
        "[HTTP -> ESP32]",
        req.method,
        req.originalUrl
    );

    // =========================================================
    // CHECK ESP32 CONNECTION
    // =========================================================

    if (
        !esp32 ||
        esp32.readyState !== WebSocket.OPEN
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

    // =========================================================
    // CREATE REQUEST ID
    // =========================================================

    const id =
        `${Date.now()}-${Math.random()
            .toString(36)
            .slice(2, 10)}`;

    // =========================================================
    // COPY IMPORTANT BROWSER HEADERS
    // =========================================================

    const headers = {};

    for (
        const [key, value]
        of Object.entries(req.headers)
    ) {

        const lower = key.toLowerCase();

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

            if (Array.isArray(value)) {

                headers[lower] =
                    value.join(", ");

            } else {

                headers[lower] =
                    String(value);
            }
        }
    }

    // =========================================================
    // REQUEST BODY -> BASE64
    // =========================================================

    let body = "";

    if (
        req.body &&
        Buffer.isBuffer(req.body) &&
        req.body.length > 0
    ) {

        body =
            req.body.toString("base64");
    }

    // =========================================================
    // 30 SECOND REQUEST TIMEOUT
    // =========================================================

    const timer = setTimeout(() => {

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
            "id:",
            id
        );

        if (!res.headersSent) {

            res
                .status(504)
                .send(
                    "ESP32 request timeout"
                );
        }

    }, 30000);

    // =========================================================
    // SAVE PENDING REQUEST
    // =========================================================

    pending.set(id, {
        res,
        timer
    });

    // =========================================================
    // MESSAGE FOR ESP32
    // =========================================================

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

    // =========================================================
    // SEND REQUEST THROUGH WEBSOCKET
    // =========================================================

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
// RENDER PORT
// ============================================================

const PORT =
    process.env.PORT || 3000;

// ============================================================
// START SERVER
// ============================================================

server.listen(PORT, () => {

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
});

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
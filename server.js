const express = require("express");
const http = require("http");
const WebSocket = require("ws");

const app = express();
const server = http.createServer(app);

// ============================================================
// ESP32 WEBSOCKET SERVER
// ============================================================

const wss = new WebSocket.Server({
    server,
    path: "/tunnel"
});

let esp32 = null;

// Browser requests waiting for ESP32 responses
const pending = new Map();

// ============================================================
// ESP32 CONNECTION
// ============================================================

wss.on("connection", (ws) => {
    console.log("[WS] ESP32 socket connected");

    ws.on("message", (message) => {
        try {
            const data = JSON.parse(message.toString());

            console.log("[ESP32 -> RENDER]", data.type);

            // ------------------------------------------------
            // ESP32 REGISTER
            // ------------------------------------------------

            if (data.type === "esp32") {
                if (esp32 && esp32 !== ws) {
                    try {
                        esp32.close();
                    } catch (err) {
                        console.error(
                            "[WS] Error closing old ESP32:",
                            err.message
                        );
                    }
                }

                esp32 = ws;

                console.log("[WS] ESP32 registered");

                try {
                    ws.send(
                        JSON.stringify({
                            type: "registered"
                        })
                    );
                } catch (err) {
                    console.error(
                        "[WS] Registration response failed:",
                        err.message
                    );
                }

                return;
            }

            // ------------------------------------------------
            // RESPONSE FROM ESP32
            // ------------------------------------------------

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

                // Decode body from Base64
                let body;

                try {
                    body = Buffer.from(
                        data.body || "",
                        "base64"
                    );
                } catch (err) {
                    console.error(
                        "[HTTP] Invalid Base64 body:",
                        err.message
                    );

                    if (!item.res.headersSent) {
                        item.res
                            .status(502)
                            .send("Invalid response body from ESP32");
                    }

                    return;
                }

                const headers = data.headers || {};

                // ------------------------------------------------
                // FORWARD ESP32 RESPONSE HEADERS
                // ------------------------------------------------

                for (const [key, value] of Object.entries(headers)) {
                    const lower = key.toLowerCase();

                    // Render must manage these itself
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

                    // Important for login session cookie
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

                    // Handle array-valued headers
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
                // CONTENT LENGTH
                // ------------------------------------------------

                item.res.setHeader(
                    "Content-Length",
                    body.length
                );

                // ------------------------------------------------
                // CACHE CONTROL
                // ------------------------------------------------

                if (!item.res.getHeader("Cache-Control")) {
                    item.res.setHeader(
                        "Cache-Control",
                        headers["cache-control"] || "no-store"
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
                    body.length
                );

                if (!item.res.headersSent) {
                    item.res.status(statusCode);
                }

                item.res.end(body);

                return;
            }

        } catch (error) {
            console.error(
                "[WS] Message error:",
                error.message
            );
        }
    });

    // ========================================================
    // ESP32 DISCONNECTED
    // ========================================================

    ws.on("close", () => {
        console.log("[WS] ESP32 socket closed");

        if (esp32 === ws) {
            esp32 = null;
        }

        // Fail all waiting browser requests
        for (const [id, item] of pending.entries()) {
            clearTimeout(item.timer);

            if (!item.res.headersSent) {
                item.res
                    .status(503)
                    .send("ESP32 disconnected");
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

app.get("/cloud-status", (req, res) => {
    res.json({
        render: true,
        esp32Connected:
            !!esp32 &&
            esp32.readyState === WebSocket.OPEN,
        pendingRequests: pending.size
    });
});

// ============================================================
// HEALTH
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
// RAW BODY PARSER
// ============================================================
//
// Required for:
// POST /login
// POST /upload
// POST /api/data
// POST /api/ota
// etc.
//
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

    // --------------------------------------------------------
    // CHECK ESP32
    // --------------------------------------------------------

    if (
        !esp32 ||
        esp32.readyState !== WebSocket.OPEN
    ) {
        console.log(
            "[HTTP] ESP32 is not connected"
        );

        return res
            .status(503)
            .send("ESP32 is not connected");
    }

    // --------------------------------------------------------
    // UNIQUE REQUEST ID
    // --------------------------------------------------------

    const id =
        `${Date.now()}-${Math.random()
            .toString(36)
            .slice(2, 10)}`;

    // --------------------------------------------------------
    // FORWARD IMPORTANT REQUEST HEADERS
    // --------------------------------------------------------

    const headers = {};

    for (const [key, value] of Object.entries(
        req.headers
    )) {
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
                headers[lower] = value.join(", ");
            } else {
                headers[lower] = String(value);
            }
        }
    }

    // --------------------------------------------------------
    // BODY -> BASE64
    // --------------------------------------------------------

    let body = "";

    if (
        req.body &&
        Buffer.isBuffer(req.body) &&
        req.body.length > 0
    ) {
        body = req.body.toString("base64");
    }

    // --------------------------------------------------------
    // REQUEST TIMEOUT
    // --------------------------------------------------------

    const timer = setTimeout(() => {
        const item = pending.get(id);

        if (!item) {
            return;
        }

        pending.delete(id);

        console.log(
            "[HTTP] Timeout:",
            req.method,
            req.originalUrl
        );

        if (!res.headersSent) {
            res
                .status(504)
                .send("ESP32 request timeout");
        }
    }, 30000);

    // --------------------------------------------------------
    // STORE REQUEST
    // --------------------------------------------------------

    pending.set(id, {
        res,
        timer
    });

    // --------------------------------------------------------
    // REQUEST MESSAGE FOR ESP32
    // --------------------------------------------------------

    const message = {
        type: "request",
        id,
        method: req.method,
        path: req.originalUrl,
        headers,
        body
    };

    // --------------------------------------------------------
    // SEND TO ESP32
    // --------------------------------------------------------

    try {
        const jsonMessage =
            JSON.stringify(message);

        esp32.send(jsonMessage);

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

server.listen(PORT, () => {
    console.log(
        `Render gateway listening on port ${PORT}`
    );

    console.log(
        `WebSocket endpoint: /tunnel`
    );
});

// ============================================================
// PROCESS ERROR HANDLING
// ============================================================

process.on("uncaughtException", (error) => {
    console.error(
        "[PROCESS] Uncaught exception:",
        error
    );
});

process.on("unhandledRejection", (error) => {
    console.error(
        "[PROCESS] Unhandled rejection:",
        error
    );
});
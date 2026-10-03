const express = require("express");
const http = require("http");
const WebSocket = require("ws");

const app = express();
const server = http.createServer(app);

// WebSocket endpoint used by ESP32
const wss = new WebSocket.Server({
    server,
    path: "/tunnel"
});

let esp32 = null;

// Store browser requests waiting for an ESP32 response
const pending = new Map();

/*
|--------------------------------------------------------------------------
| ESP32 WebSocket connection
|--------------------------------------------------------------------------
*/

wss.on("connection", (ws) => {
    console.log("[WS] ESP32 socket connected");

    ws.on("message", (message) => {
        try {
            const data = JSON.parse(message.toString());

            console.log("[ESP32 -> RENDER]", data.type);

            /*
            |--------------------------------------------------------------------------
            | ESP32 registration
            |--------------------------------------------------------------------------
            */

            if (data.type === "esp32") {
                // Close previous ESP32 connection if one exists
                if (esp32 && esp32 !== ws) {
                    try {
                        esp32.close();
                    } catch {}
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

            /*
            |--------------------------------------------------------------------------
            | ESP32 response to browser request
            |--------------------------------------------------------------------------
            */

            if (data.type === "response") {
                const item = pending.get(data.id);

                if (!item) {
                    console.log(
                        "[WS] Unknown response id:",
                        data.id
                    );
                    return;
                }

                clearTimeout(item.timer);
                pending.delete(data.id);

                // Decode Base64 response body
                const body = Buffer.from(
                    data.body || "",
                    "base64"
                );

                const headers = data.headers || {};

                /*
                |--------------------------------------------------------------------------
                | Forward ESP32 response headers
                |--------------------------------------------------------------------------
                */

                for (const [key, value] of Object.entries(headers)) {
                    const lower = key.toLowerCase();

                    // Don't forward these because Render will generate them
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

                    /*
                    |--------------------------------------------------------------------------
                    | Important for ESP32 login cookie
                    |--------------------------------------------------------------------------
                    */

                    if (lower === "set-cookie") {
                        item.res.setHeader(
                            "Set-Cookie",
                            [String(value)]
                        );
                    } else {
                        item.res.setHeader(
                            key,
                            String(value)
                        );
                    }
                }

                /*
                |--------------------------------------------------------------------------
                | Set correct response length
                |--------------------------------------------------------------------------
                */

                item.res.setHeader(
                    "Content-Length",
                    body.length
                );

                /*
                |--------------------------------------------------------------------------
                | Prevent browser caching
                |--------------------------------------------------------------------------
                */

                item.res.setHeader(
                    "Cache-Control",
                    headers["cache-control"] ||
                        "no-store"
                );

                console.log(
                    "[HTTP] -> Browser",
                    data.statusCode || 200,
                    "bytes:",
                    body.length
                );

                /*
                |--------------------------------------------------------------------------
                | Send response to browser
                |--------------------------------------------------------------------------
                */

                item.res.status(
                    Number(data.statusCode || 200)
                );

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

    /*
    |--------------------------------------------------------------------------
    | ESP32 WebSocket closed
    |--------------------------------------------------------------------------
    */

    ws.on("close", () => {
        console.log("[WS] ESP32 socket closed");

        if (esp32 === ws) {
            esp32 = null;
        }

        // Fail all pending browser requests
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

    /*
    |--------------------------------------------------------------------------
    | ESP32 WebSocket error
    |--------------------------------------------------------------------------
    */

    ws.on("error", (error) => {
        console.error(
            "[WS] Socket error:",
            error.message
        );
    });
});

/*
|--------------------------------------------------------------------------
| Cloud status endpoint
|--------------------------------------------------------------------------
|
| Open:
| https://YOUR-RENDER-URL/cloud-status
|
*/

app.get("/cloud-status", (req, res) => {
    res.json({
        render: true,
        esp32Connected:
            !!esp32 &&
            esp32.readyState === WebSocket.OPEN,
        pendingRequests: pending.size
    });
});

/*
|--------------------------------------------------------------------------
| Health endpoint
|--------------------------------------------------------------------------
*/

app.get("/health", (req, res) => {
    res.json({
        ok: true,
        esp32Connected:
            !!esp32 &&
            esp32.readyState === WebSocket.OPEN
    });
});

/*
|--------------------------------------------------------------------------
| Receive any HTTP request body
|--------------------------------------------------------------------------
|
| Needed for:
| - POST /login
| - file upload
| - API requests
| - OTA
| - other ESP32 POST requests
|
*/

app.use(
    express.raw({
        type: "*/*",
        limit: "10mb"
    })
);

/*
|--------------------------------------------------------------------------
| Main HTTP -> ESP32 proxy
|--------------------------------------------------------------------------
*/

app.use((req, res) => {
    console.log(
        "[HTTP -> ESP32]",
        req.method,
        req.originalUrl
    );

    /*
    |--------------------------------------------------------------------------
    | Check ESP32 connection
    |--------------------------------------------------------------------------
    */

    if (
        !esp32 ||
        esp32.readyState !== WebSocket.OPEN
    ) {
        return res
            .status(503)
            .send("ESP32 is not connected");
    }

    /*
    |--------------------------------------------------------------------------
    | Generate request ID
    |--------------------------------------------------------------------------
    */

    const id =
        `${Date.now()}-${Math.random()
            .toString(36)
            .slice(2, 10)}`;

    /*
    |--------------------------------------------------------------------------
    | Forward important browser headers
    |--------------------------------------------------------------------------
    */

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
            lower === "accept"
        ) {
            headers[lower] = Array.isArray(value)
                ? value.join(", ")
                : String(value);
        }
    }

    /*
    |--------------------------------------------------------------------------
    | Convert request body to Base64
    |--------------------------------------------------------------------------
    */

    let body = "";

    if (
        req.body &&
        Buffer.isBuffer(req.body) &&
        req.body.length > 0
    ) {
        body = req.body.toString("base64");
    }

    /*
    |--------------------------------------------------------------------------
    | Request timeout
    |--------------------------------------------------------------------------
    */

    const timer = setTimeout(() => {
        const item = pending.get(id);

        if (!item) {
            return;
        }

        pending.delete(id);

        if (!res.headersSent) {
            res
                .status(504)
                .send("ESP32 request timeout");
        }

        console.log(
            "[HTTP] Request timeout:",
            id
        );
    }, 30000);

    /*
    |--------------------------------------------------------------------------
    | Save pending request
    |--------------------------------------------------------------------------
    */

    pending.set(id, {
        res,
        timer
    });

    /*
    |--------------------------------------------------------------------------
    | Message sent to ESP32
    |--------------------------------------------------------------------------
    */

    const message = {
        type: "request",
        id,
        method: req.method,
        path: req.originalUrl,
        headers,
        body
    };

    /*
    |--------------------------------------------------------------------------
    | Send request through WebSocket
    |--------------------------------------------------------------------------
    */

    try {
        esp32.send(
            JSON.stringify(message)
        );

        console.log(
            "[WS] Request sent to ESP32:",
            req.method,
            req.originalUrl
        );

    } catch (error) {
        clearTimeout(timer);
        pending.delete(id);

        console.error(
            "[WS] Send failed:",
            error.message
        );

        return res
            .status(502)
            .send(
                "Failed to send request to ESP32"
            );
    }
});

/*
|--------------------------------------------------------------------------
| Render port
|--------------------------------------------------------------------------
*/

const PORT =
    process.env.PORT || 3000;

/*
|--------------------------------------------------------------------------
| Start HTTP server
|--------------------------------------------------------------------------
*/

server.listen(PORT, () => {
    console.log(
        `Render gateway listening on port ${PORT}`
    );
});

/*
|--------------------------------------------------------------------------
| Process errors
|--------------------------------------------------------------------------
*/

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
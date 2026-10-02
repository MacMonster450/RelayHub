const express = require("express");
const http = require("http");
const WebSocket = require("ws");

const app = express();
const server = http.createServer(app);

const PORT = process.env.PORT || 3000;

// ============================================================
// ESP32 WEBSOCKET TUNNEL
// ============================================================

const wss = new WebSocket.Server({
    server,
    path: "/tunnel"
});

let esp32 = null;

// Request ID counter
let requestCounter = 0;

// Requests waiting for ESP32 responses
const pendingRequests = new Map();

// ============================================================
// BASIC INFO
// ============================================================

app.get("/api/status", (req, res) => {

    res.json({
        server: "ESP32 Reverse Tunnel",
        esp32Connected:
            esp32 !== null &&
            esp32.readyState === WebSocket.OPEN,
        pendingRequests: pendingRequests.size,
        time: new Date().toISOString()
    });

});

// ============================================================
// WEBSOCKET CONNECTION FROM ESP32
// ============================================================

wss.on("connection", (ws) => {

    console.log("ESP32 tunnel connected");

    // Disconnect previous ESP32
    if (esp32) {

        try {
            esp32.close();
        } catch (error) {}

    }

    esp32 = ws;

    ws.send(JSON.stringify({
        type: "connected",
        message: "Tunnel established"
    }));

    // --------------------------------------------------------
    // MESSAGE FROM ESP32
    // --------------------------------------------------------

    ws.on("message", (message) => {

        try {

            const data =
                JSON.parse(message.toString());

            console.log(
                "ESP32 message:",
                data.type
            );

            // ------------------------------------------------
            // ESP32 RESPONSE
            // ------------------------------------------------

            if (
                data.type === "response" &&
                data.id
            ) {

                const pending =
                    pendingRequests.get(data.id);

                if (!pending) {
                    console.log(
                        "Unknown request ID:",
                        data.id
                    );
                    return;
                }

                pendingRequests.delete(data.id);

                clearTimeout(
                    pending.timeout
                );

                // Send response back to browser
                const statusCode =
                    Number(data.statusCode) || 200;

                const headers =
                    data.headers || {};

                // Decode response body
                let body = Buffer.alloc(0);

                if (data.body) {

                    body =
                        Buffer.from(
                            data.body,
                            "base64"
                        );

                }

                resSafe(
                    pending.res,
                    statusCode,
                    headers,
                    body
                );

                return;
            }

        } catch (error) {

            console.log(
                "ESP32 message error:",
                error.message
            );

        }

    });

    // --------------------------------------------------------
    // DISCONNECT
    // --------------------------------------------------------

    ws.on("close", () => {

        console.log(
            "ESP32 tunnel disconnected"
        );

        if (esp32 === ws) {
            esp32 = null;
        }

        // Fail all waiting requests
        for (
            const [id, pending]
            of pendingRequests
        ) {

            clearTimeout(
                pending.timeout
            );

            try {

                pending.res
                    .status(503)
                    .send(
                        "ESP32 disconnected"
                    );

            } catch (error) {}

            pendingRequests.delete(id);
        }

    });

    // --------------------------------------------------------
    // ERROR
    // --------------------------------------------------------

    ws.on("error", (error) => {

        console.log(
            "ESP32 WebSocket error:",
            error.message
        );

    });

});

// ============================================================
// SAFE HTTP RESPONSE
// ============================================================

function resSafe(
    res,
    statusCode,
    headers,
    body
) {

    if (res.headersSent) {
        return;
    }

    // Do not forward hop-by-hop headers
    const blockedHeaders = new Set([
        "connection",
        "keep-alive",
        "transfer-encoding",
        "upgrade",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer"
    ]);

    for (
        const [name, value]
        of Object.entries(headers)
    ) {

        if (
            blockedHeaders.has(
                name.toLowerCase()
            )
        ) {
            continue;
        }

        try {

            res.setHeader(
                name,
                value
            );

        } catch (error) {}

    }

    res.status(statusCode);

    res.send(body);
}

// ============================================================
// CREATE REQUEST ID
// ============================================================

function createRequestID() {

    requestCounter++;

    return (
        Date.now().toString(36) +
        "-" +
        requestCounter.toString(36)
    );
}

// ============================================================
// REQUEST BODY
// ============================================================

app.use(
    express.raw({
        type: "*/*",
        limit: "20mb"
    })
);

// ============================================================
// PUBLIC HTTP → ESP32 PROXY
// ============================================================

app.use(async (req, res) => {

    // --------------------------------------------------------
    // CHECK ESP32
    // --------------------------------------------------------

    if (
        !esp32 ||
        esp32.readyState !== WebSocket.OPEN
    ) {

        return res.status(503).send(`
<!DOCTYPE html>
<html>
<head>
    <title>ESP32 Offline</title>
</head>

<body style="
    background:#080c15;
    color:white;
    font-family:Arial;
    text-align:center;
    padding-top:100px;
">

    <h1>ESP32 Offline</h1>

    <p>
        The ESP32 is not connected to the cloud tunnel.
    </p>

</body>
</html>
        `);

    }

    // --------------------------------------------------------
    // CREATE REQUEST ID
    // --------------------------------------------------------

    const id =
        createRequestID();

    // --------------------------------------------------------
    // COPY REQUEST HEADERS
    // --------------------------------------------------------

    const headers = {};

    for (
        const [name, value]
        of Object.entries(req.headers)
    ) {

        const lower =
            name.toLowerCase();

        // Skip headers that belong
        // only to Render's connection
        if (
            lower === "host" ||
            lower === "connection" ||
            lower === "content-length"
        ) {
            continue;
        }

        headers[name] = value;

    }

    // --------------------------------------------------------
    // REQUEST BODY
    // --------------------------------------------------------

    let bodyBase64 = "";

    if (
        req.body &&
        Buffer.isBuffer(req.body) &&
        req.body.length > 0
    ) {

        bodyBase64 =
            req.body.toString("base64");

    }

    // --------------------------------------------------------
    // REQUEST MESSAGE
    // --------------------------------------------------------

    const requestMessage = {

        type: "request",

        id: id,

        method:
            req.method,

        path:
            req.originalUrl,

        headers:
            headers,

        body:
            bodyBase64

    };

    // --------------------------------------------------------
    // WAIT FOR ESP32 RESPONSE
    // --------------------------------------------------------

    const timeout =
        setTimeout(() => {

            if (
                pendingRequests.has(id)
            ) {

                pendingRequests.delete(id);

                if (
                    !res.headersSent
                ) {

                    res
                        .status(504)
                        .send(
                            "ESP32 request timeout"
                        );

                }

            }

        }, 30000);

    pendingRequests.set(
        id,
        {
            res: res,
            timeout: timeout
        }
    );

    // --------------------------------------------------------
    // SEND REQUEST TO ESP32
    // --------------------------------------------------------

    try {

        esp32.send(
            JSON.stringify(
                requestMessage
            )
        );

        console.log(
            `${req.method} ${req.originalUrl} -> ESP32`
        );

    } catch (error) {

        clearTimeout(timeout);

        pendingRequests.delete(id);

        return res
            .status(502)
            .send(
                "Failed to send request to ESP32"
            );

    }

});

// ============================================================
// START SERVER
// ============================================================

server.listen(
    PORT,
    "0.0.0.0",
    () => {

        console.log(
            `ESP32 tunnel server running on port ${PORT}`
        );

    }
);
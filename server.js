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


// ========================================
// ESP32 WEBSOCKET
// ========================================

wss.on("connection", (ws) => {

    console.log("[WS] ESP32 socket connected");

    ws.on("message", (message) => {

        try {

            const data =
                JSON.parse(message.toString());

            console.log(
                "[ESP32 -> RENDER]",
                data.type
            );


            // ESP32 registration
            if (data.type === "esp32") {

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


            // HTTP response from ESP32
            if (data.type === "response") {

                const request =
                    pending.get(data.id);

                if (!request) {

                    console.log(
                        "[WS] Unknown response ID:",
                        data.id
                    );

                    return;
                }

                pending.delete(data.id);

                const statusCode =
                    data.statusCode || 200;

                const headers =
                    data.headers || {};

                const body =
                    Buffer.from(
                        data.body || "",
                        "base64"
                    );


                // Forward headers
                for (
                    const [key, value]
                    of Object.entries(headers)
                ) {

                    if (
                        key.toLowerCase() ===
                        "content-length"
                    ) {
                        continue;
                    }

                    try {
                        request.res.set(
                            key,
                            value
                        );
                    }
                    catch {}
                }


                request.res.status(
                    statusCode
                );

                request.res.end(body);

                console.log(
                    "[HTTP] Response:",
                    statusCode,
                    "bytes:",
                    body.length
                );

                return;
            }

        }
        catch (error) {

            console.error(
                "[WS] Message error:",
                error.message
            );
        }

    });


    ws.on("close", () => {

        console.log(
            "[WS] ESP32 disconnected"
        );

        if (esp32 === ws) {
            esp32 = null;
        }

        // Fail pending requests
        for (
            const [id, request]
            of pending
        ) {

            try {

                request.res
                    .status(503)
                    .send(
                        "ESP32 disconnected"
                    );

            }
            catch {}

            pending.delete(id);
        }

    });

});


// ========================================
// PROXY ALL HTTP REQUESTS TO ESP32
// ========================================

app.use(
    express.raw({
        type: "*/*",
        limit: "2mb"
    })
);


app.use(async (req, res) => {

    console.log(
        "[HTTP]",
        req.method,
        req.originalUrl
    );


    // Don't proxy the tunnel itself
    if (
        req.path === "/tunnel"
    ) {
        return res.status(404).end();
    }


    // ESP32 must be connected
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


    const id =
        `${Date.now()}-${Math.random()
            .toString(36)
            .slice(2, 8)}`;


    // ------------------------------------
    // Headers
    // ------------------------------------

    const headers = {};


    for (
        const [key, value]
        of Object.entries(req.headers)
    ) {

        const lower =
            key.toLowerCase();


        // Only forward useful headers
        if (
            lower === "cookie" ||
            lower === "x-api-key" ||
            lower === "content-type" ||
            lower === "user-agent" ||
            lower === "authorization"
        ) {

            headers[lower] =
                Array.isArray(value)
                    ? value.join(", ")
                    : value;
        }

    }


    // ------------------------------------
    // Body
    // ------------------------------------

    let body = "";

    if (
        req.body &&
        Buffer.isBuffer(req.body)
    ) {

        body =
            req.body.toString("base64");
    }


    // ------------------------------------
    // Path
    // ------------------------------------

    const path =
        req.originalUrl;


    // ------------------------------------
    // Store request
    // ------------------------------------

    pending.set(id, {
        res,
        timer: null
    });


    // ------------------------------------
    // Timeout
    // ------------------------------------

    const timer =
        setTimeout(() => {

            const request =
                pending.get(id);

            if (!request) {
                return;
            }

            pending.delete(id);

            if (!res.headersSent) {

                res
                    .status(504)
                    .send(
                        "ESP32 request timeout"
                    );
            }

        }, 20000);


    pending.get(id).timer = timer;


    // ------------------------------------
    // Send to ESP32
    // ------------------------------------

    const message = {

        type: "request",

        id,

        method:
            req.method,

        path,

        headers,

        body

    };


    console.log(
        "[RENDER -> ESP32]",
        req.method,
        path
    );


    try {

        esp32.send(
            JSON.stringify(message)
        );

    }
    catch (error) {

        clearTimeout(timer);

        pending.delete(id);

        return res
            .status(502)
            .send(
                "Failed to send request to ESP32"
            );
    }

});


// ========================================
// RENDER STATUS
// ========================================

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


// ========================================
// SERVER
// ========================================

const PORT =
    process.env.PORT || 3000;

server.listen(
    PORT,
    () => {

        console.log(
            `Server running on port ${PORT}`
        );

    }
);
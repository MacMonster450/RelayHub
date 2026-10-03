const express = require("express");
const http = require("http");
const WebSocket = require("ws");

const app = express();
const server = http.createServer(app);

const wss = new WebSocket.Server({
    server,
    path: "/tunnel",
    maxPayload: 2 * 1024 * 1024
});

let esp32 = null;
const pending = new Map();

function clearPending(id) {
    const item = pending.get(id);

    if (!item) {
        return null;
    }

    clearTimeout(item.timer);
    pending.delete(id);

    return item;
}

function failPending(id, status, message) {
    const item = clearPending(id);

    if (!item) {
        return;
    }

    if (!item.res.headersSent) {
        item.res
            .status(status)
            .send(message);
    } else {
        try {
            item.res.end();
        } catch {}
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

    for (
        const [key, value]
        of Object.entries(headers)
    ) {
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
        headers["cache-control"] ||
        "no-store"
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
}

function sendWs(ws, payload) {
    return new Promise((resolve, reject) => {
        if (
            !ws ||
            ws.readyState !==
                WebSocket.OPEN
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

// ============================================================
// MULTIPART FILE PARSER
// ============================================================

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
        /boundary=(?:"([^"]+)"|([^;]+))/i
            .exec(contentType || "");

    if (!match) {
        throw new Error(
            "Multipart boundary not found"
        );
    }

    const boundary =
        match[1] || match[2];

    const marker = Buffer.from(
        `--${boundary}`,
        "utf8"
    );

    let cursor =
        body.indexOf(marker);

    while (cursor >= 0) {
        let partStart =
            cursor + marker.length;

        // Multipart finished
        if (
            body
                .slice(
                    partStart,
                    partStart + 2
                )
                .toString() ===
            "--"
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
                Buffer.from("\r\n\r\n"),
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
            /content-disposition:\s*([^\r\n]+)/i
                .exec(headersText);

        if (disposition) {
            const value =
                disposition[1];

            const filenameMatch =
                /filename="([^"]*)"/i
                    .exec(value);

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

// ============================================================
// CLOUD UPLOAD
// ============================================================

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
        esp32.readyState !==
            WebSocket.OPEN
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
        file = parseMultipartFile(
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

    try {
        console.log(
            "[CLOUD UPLOAD] File:",
            file.filename,
            "bytes:",
            file.data.length
        );

        // Tell ESP32 a file upload is starting
        await sendWs(
            esp32,
            {
                type: "upload_start",
                id,
                filename:
                    file.filename,
                cookie
            }
        );

        // Keep WebSocket messages small
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
                esp32,
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

        // Tell ESP32 upload is complete
        await sendWs(
            esp32,
            {
                type:
                    "upload_end",

                id,

                totalBytes:
                    file.data.length
            }
        );

    } catch (error) {
        console.error(
            "[CLOUD UPLOAD] Transfer error:",
            error.message
        );

        failPending(
            id,
            502,
            "Failed to send upload to ESP32"
        );
    }
}

// ============================================================
// ESP32 WEBSOCKET
// ============================================================

wss.on("connection", (ws) => {
    console.log(
        "[WS] ESP32 socket connected"
    );

    ws.on("message", (message) => {
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

            // =================================================
            // ESP32 REGISTER
            // =================================================

            if (
                data.type ===
                "esp32"
            ) {
                if (
                    esp32 &&
                    esp32 !== ws
                ) {
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

                return;
            }

            // =================================================
            // LEGACY RESPONSE
            // =================================================

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
                    data.headers || {};

                sendPendingResponse(
                    data.id,
                    item,
                    body
                );

                return;
            }

            // =================================================
            // RESPONSE START
            // =================================================

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

            // =================================================
            // RESPONSE CHUNK
            // =================================================

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
                    chunk.length > 0
                ) {
                    item.chunks.push(
                        chunk
                    );

                    item.receivedBytes +=
                        chunk.length;
                }

                return;
            }

            // =================================================
            // RESPONSE END
            // =================================================

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
                        item.chunks || []
                    );

                console.log(
                    "[TUNNEL] Response end",
                    "received:",
                    body.length,
                    "reported:",
                    Number(
                        data.totalBytes || 0
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

            // =================================================
            // CLOUD UPLOAD COMPLETE RESPONSE
            // =================================================

            if (
                data.type ===
                "upload_response"
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
                    data.headers || {};

                let body =
                    Buffer.alloc(0);

                try {
                    if (
                        data.body
                    ) {
                        body =
                            Buffer.from(
                                data.body,
                                "base64"
                            );
                    }
                } catch {
                    failPending(
                        data.id,
                        502,
                        "Invalid upload response from ESP32"
                    );

                    return;
                }

                sendPendingResponse(
                    data.id,
                    item,
                    body
                );

                return;
            }

            // =================================================
            // UPLOAD ERROR
            // =================================================

            if (
                data.type ===
                "upload_error"
            ) {
                failPending(
                    data.id,
                    Number(
                        data.statusCode ||
                            502
                    ),
                    data.message ||
                        "ESP32 upload failed"
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

    // =========================================================
    // CONNECTION CLOSED
    // =========================================================

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

    // =========================================================
    // WEBSOCKET ERROR
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
// CLOUD FILE UPLOAD
// ============================================================

app.post(
    "/upload",
    (req, res) => {
        handleCloudUpload(
            req,
            res
        ).catch((error) => {
            console.error(
                "[CLOUD UPLOAD] Unexpected error:",
                error
            );

            if (!res.headersSent) {
                res
                    .status(500)
                    .send(
                        "Upload failed"
                    );
            }
        });
    }
);

// ============================================================
// NORMAL HTTP -> ESP32
// ============================================================

app.use((req, res) => {
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

    // --------------------------------------------------------
    // Forward browser headers
    // --------------------------------------------------------

    const headers = {};

    for (
        const [key, value]
        of Object.entries(
            req.headers
        )
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

    // --------------------------------------------------------
    // Body -> Base64
    // --------------------------------------------------------

    let body = "";

    if (
        req.body &&
        Buffer.isBuffer(req.body) &&
        req.body.length > 0
    ) {
        body =
            req.body.toString(
                "base64"
            );
    }

    // --------------------------------------------------------
    // Timeout
    // --------------------------------------------------------

    const timer =
        setTimeout(() => {
            const item =
                pending.get(id);

            if (!item) {
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
        }, 30000);

    // --------------------------------------------------------
    // Pending request
    // --------------------------------------------------------

    pending.set(id, {
        res,
        timer,
        finished: false,
        chunks: [],
        receivedBytes: 0,
        statusCode: 200,
        headers: {}
    });

    // --------------------------------------------------------
    // Message to ESP32
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
    // Send
    // --------------------------------------------------------

    try {
        esp32.send(
            JSON.stringify(
                message
            )
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

// ============================================================
// ERROR HANDLING
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
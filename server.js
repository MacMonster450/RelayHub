const express = require("express");
const http = require("http");
const WebSocket = require("ws");

const app = express();
const server = http.createServer(app);

const wss = new WebSocket.Server({
    server,
    path: "/tunnel"
});

const PORT = process.env.PORT || 3000;

// Only one ESP32 tunnel for now
let esp32 = null;

app.use(express.static("public"));

/*
============================================================
STATUS
============================================================
*/

app.get("/api/status", (req, res) => {

    res.json({
        server: "ESP32 Reverse Tunnel",
        esp32Connected:
            esp32 !== null &&
            esp32.readyState === WebSocket.OPEN,
        time: new Date().toISOString()
    });

});

/*
============================================================
WEBSOCKET TUNNEL
============================================================
*/

wss.on("connection", (ws) => {

    console.log("ESP32 tunnel connected");

    // Replace old connection
    if (esp32) {

        try {
            esp32.close();
        } catch (e) {}

    }

    esp32 = ws;

    ws.send(JSON.stringify({
        type: "connected",
        message: "Tunnel established"
    }));

    ws.on("message", (message) => {

        console.log(
            "Received from ESP32:",
            message.toString()
        );

    });

    ws.on("close", () => {

        console.log("ESP32 tunnel disconnected");

        if (esp32 === ws) {
            esp32 = null;
        }

    });

    ws.on("error", (error) => {

        console.log(
            "ESP32 WebSocket error:",
            error.message
        );

    });

});

/*
============================================================
HOME
============================================================
*/

app.get("/", (req, res) => {

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
        The ESP32 has not connected to the cloud tunnel yet.
    </p>

</body>
</html>
        `);

    }

    res.send(`
<!DOCTYPE html>
<html>
<head>
    <title>ESP32 Tunnel</title>
</head>

<body style="
    background:#080c15;
    color:white;
    font-family:Arial;
    text-align:center;
    padding-top:100px;
">

    <h1>ESP32 Connected</h1>

    <p>
        Cloud tunnel is active.
    </p>

    <p>
        Next step will forward the ESP32 WebServer.
    </p>

</body>
</html>
    `);

});

/*
============================================================
SERVER
============================================================
*/

server.listen(
    PORT,
    "0.0.0.0",
    () => {

        console.log(
            `Tunnel server running on port ${PORT}`
        );

    }
);
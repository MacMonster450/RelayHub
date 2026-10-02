const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const cors = require("cors");

const app = express();
const server = http.createServer(app);

const wss = new WebSocket.Server({
    server
});

const PORT = process.env.PORT || 3000;

// Middleware
app.use(cors());
app.use(express.json());

// Admin website
app.use(express.static("public"));

// Connected ESP32 devices
const devices = new Map();


// ========================================
// SERVER STATUS
// ========================================

app.get("/api/status", (req, res) => {

    res.json({
        server: "ESP32 Cloud Server",
        status: "online",
        devices: devices.size,
        time: new Date().toISOString()
    });

});


// ========================================
// GET DEVICES
// ========================================

app.get("/api/devices", (req, res) => {

    const deviceList = [];

    for (const [deviceId, device] of devices) {

        deviceList.push({
            deviceId: deviceId,
            relay1: device.relay1,
            relay2: device.relay2,
            relay3: device.relay3,
            connected: true,
            lastSeen: device.lastSeen
        });

    }

    res.json(deviceList);

});


// ========================================
// WEBSOCKET CONNECTION
// ========================================

wss.on("connection", (ws) => {

    console.log("WebSocket client connected");

    let deviceId = null;


    // ------------------------------------
    // RECEIVE MESSAGE
    // ------------------------------------

    ws.on("message", (message) => {

        try {

            const data = JSON.parse(message);

            console.log("Received:", data);


            // ==============================
            // DEVICE REGISTER
            // ==============================

            if (data.type === "register") {

                deviceId = data.deviceId;

                devices.set(deviceId, {

                    ws: ws,

                    deviceId: deviceId,

                    relay1: false,

                    relay2: false,

                    relay3: false,

                    lastSeen: Date.now()

                });

                ws.send(JSON.stringify({

                    type: "registered",

                    deviceId: deviceId

                }));

                console.log(
                    "ESP32 registered:",
                    deviceId
                );

            }


            // ==============================
            // STATUS UPDATE
            // ==============================

            if (data.type === "status") {

                if (deviceId && devices.has(deviceId)) {

                    const device =
                        devices.get(deviceId);

                    device.relay1 =
                        Boolean(data.relay1);

                    device.relay2 =
                        Boolean(data.relay2);

                    device.relay3 =
                        Boolean(data.relay3);

                    device.lastSeen =
                        Date.now();

                    devices.set(
                        deviceId,
                        device
                    );

                }

            }


            // ==============================
            // HEARTBEAT
            // ==============================

            if (data.type === "heartbeat") {

                if (deviceId && devices.has(deviceId)) {

                    devices.get(deviceId).lastSeen =
                        Date.now();

                }

            }

        } catch (error) {

            console.log(
                "Invalid WebSocket message:",
                error.message
            );

        }

    });


    // ------------------------------------
    // DISCONNECT
    // ------------------------------------

    ws.on("close", () => {

        console.log(
            "WebSocket disconnected:",
            deviceId
        );

        if (deviceId) {

            devices.delete(deviceId);

        }

    });

});


// ========================================
// SEND RELAY COMMAND
// ========================================

app.post("/api/relay", (req, res) => {

    const {
        deviceId,
        relay,
        state
    } = req.body;


    // Check device ID
    if (!deviceId) {

        return res.status(400).json({

            success: false,

            error: "deviceId is required"

        });

    }


    // Check relay number
    if (![1, 2, 3].includes(Number(relay))) {

        return res.status(400).json({

            success: false,

            error: "Relay must be 1, 2 or 3"

        });

    }


    // Check device
    if (!devices.has(deviceId)) {

        return res.status(404).json({

            success: false,

            error: "ESP32 is offline"

        });

    }


    const device =
        devices.get(deviceId);


    // Command
    const command = {

        type: "relay",

        relay: Number(relay),

        state: Boolean(state)

    };


    // Send to ESP32
    device.ws.send(
        JSON.stringify(command)
    );


    console.log(
        "Relay command:",
        command
    );


    res.json({

        success: true,

        command: command

    });

});


// ========================================
// START SERVER
// ========================================

server.listen(
    PORT,
    "0.0.0.0",
    () => {

        console.log(
            `ESP32 Cloud Server running on port ${PORT}`
        );

    }
);
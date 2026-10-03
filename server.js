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

let pendingRequest = null;


// ========================================
// ESP32 WEBSOCKET
// ========================================

wss.on("connection", (ws) => {

    console.log("WebSocket client connected");


    ws.on("message", (message) => {

        try {

            const data =
                JSON.parse(message.toString());


            console.log(
                "ESP32 -> Render:",
                data.type
            );


            // -----------------------------
            // ESP32 REGISTER
            // -----------------------------

            if (data.type === "esp32") {

                esp32 = ws;

                console.log(
                    "ESP32 registered"
                );

                ws.send(
                    JSON.stringify({
                        type: "registered"
                    })
                );

                return;
            }


            // -----------------------------
            // HTTP RESPONSE
            // -----------------------------

            if (
                data.type === "http_response"
            ) {

                console.log(
                    "HTTP response received from ESP32"
                );


                if (
                    pendingRequest
                ) {

                    const request =
                        pendingRequest;

                    pendingRequest = null;


                    resSend(
                        request.res,
                        data
                    );
                }

                return;
            }


            // -----------------------------
            // PONG
            // -----------------------------

            if (data.type === "pong") {

                console.log(
                    "Pong received"
                );

                return;
            }

        }
        catch (error) {

            console.log(
                "Invalid WebSocket message"
            );

            console.log(
                error.message
            );
        }

    });


    ws.on("close", () => {

        console.log(
            "WebSocket disconnected"
        );


        if (esp32 === ws) {

            esp32 = null;

            console.log(
                "ESP32 disconnected"
            );
        }

    });

});


// ========================================
// SEND RESPONSE TO BROWSER
// ========================================

function resSend(res, data) {

    const status =
        data.status || 200;


    const contentType =
        data.contentType ||
        "text/plain";


    res.status(status);

    res.set(
        "Content-Type",
        contentType
    );


    res.send(
        data.body || ""
    );
}


// ========================================
// HOME PAGE
// ========================================

app.get("/", (req, res) => {

    res.send(`
<!DOCTYPE html>

<html>

<head>

<title>ESP32 Cloud Tunnel</title>

<style>

body {
    background:#111;
    color:white;
    font-family:Arial;
    text-align:center;
    padding:50px;
}

button {
    padding:15px 30px;
    font-size:18px;
    cursor:pointer;
}

#result {
    margin-top:30px;
}

</style>

</head>

<body>

<h1>ESP32 Cloud Tunnel</h1>

<p>
Render → ESP32 Loopback Test
</p>

<button onclick="test()">
Test ESP32 HTTP
</button>

<div id="result">
Waiting...
</div>

<script>

async function test() {

    const result =
        document.getElementById("result");

    result.innerHTML =
        "Requesting ESP32...";

    try {

        const response =
            await fetch("/esp32-test");

        const html =
            await response.text();

        result.innerHTML = html;

    }
    catch(error) {

        result.innerHTML =
            "ERROR: " + error;

    }

}

</script>

</body>

</html>
`);

});


// ========================================
// ESP32 LOOPBACK TEST
// ========================================

app.get("/esp32-test", (req, res) => {

    console.log(
        "Browser requested ESP32"
    );


    // Check ESP32
    if (!esp32) {

        return res.status(503).send(
            "ESP32 is not connected"
        );

    }


    // Prevent multiple requests
    if (pendingRequest) {

        return res.status(429).send(
            "Another request is already running"
        );

    }


    pendingRequest = {
        res: res
    };


    // ------------------------------------
    // SEND LOOPBACK TEST TO ESP32
    // ------------------------------------

    const message = {

        type: "loopback_test"

    };


    console.log(
        "Render -> ESP32:",
        message
    );


    esp32.send(
        JSON.stringify(message)
    );


    // ------------------------------------
    // TIMEOUT
    // ------------------------------------

    setTimeout(() => {

        if (pendingRequest) {

            pendingRequest = null;

            try {

                res.status(504).send(
                    "ESP32 response timeout"
                );

            }
            catch (error) {

                console.log(
                    "Response already closed"
                );
            }

        }

    }, 15000);

});


// ========================================
// STATUS
// ========================================

app.get("/api/status", (req, res) => {

    res.json({

        render: true,

        esp32Connected:
            !!esp32

    });

});


// ========================================
// START
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
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

// ========================================
// ESP32 WEBSOCKET CONNECTION
// ========================================

wss.on("connection", (ws) => {
  console.log("WebSocket client connected");

  ws.on("message", (message) => {
    try {
      const data = JSON.parse(message.toString());

      console.log("ESP32 -> Render:", data);

      // ESP32 identifies itself
      if (data.type === "esp32") {
        esp32 = ws;

        console.log("ESP32 registered");

        ws.send(JSON.stringify({
          type: "registered",
          message: "ESP32 connected successfully"
        }));

        return;
      }

      // Response from ESP32
      if (data.type === "response") {
        console.log("Response received from ESP32");

        return;
      }

      // Pong from ESP32
      if (data.type === "pong") {
        console.log("Pong received from ESP32");
        return;
      }

    } catch (error) {
      console.log("Invalid message:", message.toString());
    }
  });

  ws.on("close", () => {
    console.log("WebSocket disconnected");

    if (esp32 === ws) {
      esp32 = null;
      console.log("ESP32 disconnected");
    }
  });
});


// ========================================
// RENDER TEST PAGE
// ========================================

app.get("/", (req, res) => {

  res.send(`
<!DOCTYPE html>
<html>
<head>
    <title>ESP32 Cloud Relay</title>

    <style>
        body {
            font-family: Arial;
            background: #111;
            color: white;
            text-align: center;
            padding: 50px;
        }

        .box {
            max-width: 600px;
            margin: auto;
            background: #222;
            padding: 30px;
            border-radius: 15px;
        }

        button {
            padding: 12px 25px;
            font-size: 16px;
            cursor: pointer;
        }

        #result {
            margin-top: 20px;
            padding: 15px;
            background: #000;
            border-radius: 10px;
        }
    </style>
</head>

<body>

<div class="box">

    <h1>ESP32 Cloud Relay</h1>

    <p>
        Render Gateway Test
    </p>

    <button onclick="testESP32()">
        Test ESP32
    </button>

    <div id="result">
        Waiting...
    </div>

</div>

<script>

async function testESP32() {

    const result =
        document.getElementById("result");

    result.innerText = "Testing...";

    try {

        const response =
            await fetch("/test");

        const text =
            await response.text();

        result.innerText = text;

    } catch (error) {

        result.innerText =
            "Error: " + error;

    }

}

</script>

</body>
</html>
  `);

});


// ========================================
// TEST ESP32 CONNECTION
// ========================================

app.get("/test", (req, res) => {

  if (!esp32) {

    return res.status(503).send(
      "ESP32 is not connected to Render"
    );

  }

  const requestId =
    Date.now().toString();

  const message = {

    type: "test",

    id: requestId

  };

  console.log(
    "Render -> ESP32:",
    message
  );

  esp32.send(
    JSON.stringify(message)
  );

  res.send(
    "Request sent to ESP32. Check ESP32 Serial Monitor."
  );

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
// START SERVER
// ========================================

const PORT =
  process.env.PORT || 3000;

server.listen(PORT, () => {

  console.log(
    `Server running on port ${PORT}`
  );

});
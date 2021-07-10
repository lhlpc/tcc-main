// ---------- My Influx DB code
// const {InfluxDB} = require('@influxdata/influxdb-client')

// const token = 'yptM0svoMLVLyjDlCHV3367er1eR3IEpFS4Xiv6pr3Ljco-T22XW0x3yn2RffbheOmir8RanDw4V6-agkXXN5Q=='
// const org = 'lpcluizhenrique@gmail.com'
// const bucket = `cf33074baad6335d`

// const client = new InfluxDB({url: 'https://us-west-2-1.aws.cloud2.influxdata.com', token: token})

// const { Point } = require('@influxdata/influxdb-client')
// const writeApi = client.getWriteApi(org, bucket)
// writeApi.useDefaultTags({host: 'host1'})

// ------- MySQL
const mysql = require("mysql2/promise");

let relationalDatabaseConnection;

// ------- END MySQL

const webSocketClients = [];

async function storeSignal(idUser, signalType, signalLevel) {
  try {
    const {
      InfluxDB,
      DEFAULT_RetryDelayStrategyOptions,
    } = require("@influxdata/influxdb-client");

    const token =
      "f3hMIrt80eERkkvLclrJAH7rTPW5ZXhbo0LYGsXKJqzQUYvR62QlyEG-_Ywx5-uHOynFeTq5gKksoNeJmnghXg==";
    const org = "lpcluizhenrique@gmail.com";
    const bucket = `emg_signals`;

    const client = new InfluxDB({
      url: "https://us-west-2-1.aws.cloud2.influxdata.com",
      token: token,
    });

    const { Point } = require("@influxdata/influxdb-client");
    const writeApi = client.getWriteApi(org, bucket);
    writeApi.useDefaultTags({ host: "host1" });
    // const point = new Point('emg')
    //    .floatField('signal_level', 23.43234543)
    //    .tag('id_user', '2');

    console.log(signalType, signalLevel, idUser);

    const point = new Point(signalType)
      .floatField("signal_level", signalLevel)
      .tag("id_user", idUser);

    writeApi.writePoint(point);
    // writeApi
    //     .close()
    //     .then(() => {
    //         console.log('FINISHED')
    //     })
    //     .catch(e => {
    //         console.error(e)
    //         console.log('\\nFinished ERROR')
    //     });

    await writeApi.close();
    console.log("FINISHED");
  } catch (error) {
    console.error(error);
    console.log("\\nFinished ERROR");
  }
}
// --------------------------------------------

async function processSignal(signal) {
  // Check structure
  console.log(signal.toString());
  console.log(JSON.parse(signal));
  const { idUser, signalSamples } = JSON.parse(signal);

  for (const signalSample of signalSamples) {
    console.log(idUser, signalSample);
    // await storeSignal(idUser, "emg", signalSample);
  }

  // -- Check if id_user exists on MySQL
  const [[{ userExists }]] = await relationalDatabaseConnection.execute(
    `SELECT count(*) as userExists  from user where id_user = ${idUser}`
  );

  console.log("UserExists:", userExists);

  if (userExists !== 1) {
    return 0;
  }

  // -- Stores on InfluxDB
  //...

  // -- Transmit to listening websocket
  webSocketClients[0].sendUTF(signalSamples); // connection.sendUTF(message.utf8Data);
  console.log("Transmitted to websocket");
}

//---------------------------------------------------- MQTT Subscriber Code
const mqtt = require("mqtt");
const exp = require("constants");
(async () => {
  try {
    relationalDatabaseConnection = await mysql.createConnection({
      host: "sql10.freemysqlhosting.net",
      user: "sql10424166",
      password: "CM4GsnJ1KP",
      database: "sql10424166",
      connectTimeout: 40000,
    });
    console.log(`Connection with relational database was established.`);

    const BROKER_URL = "mqtt://broker.hivemq.com";
    const MQTT_TOPIC = "lpctcc/emg";
    const mqttClient = mqtt.connect(BROKER_URL);

    mqttClient.on("connect", () => {
      mqttClient.subscribe(MQTT_TOPIC);
      console.log(
        `Subscribed to ${MQTT_TOPIC} MQTT topic on broker ${BROKER_URL}`
      );
    });

    mqttClient.on("message", async (topic, message) => {
      if (topic === MQTT_TOPIC) {
        await processSignal(message);
        return;
      }
      console.log("No handler for topic %s", topic);
    });
    // ----------------------
    // ----------------------
    // ---------------------- WebSocket

    var WebSocketServer = require("websocket").server;
    var http = require("http");

    var server = http.createServer(function (request, response) {
      console.log(new Date() + " Received request for " + request.url);
      response.writeHead(404);
      response.end();
    });
    server.listen(8000, function () {
      console.log(new Date() + " Server is listening on port 8000");
    });

    wsServer = new WebSocketServer({
      httpServer: server,
      // You should not use autoAcceptConnections for production
      // applications, as it defeats all standard cross-origin protection
      // facilities built into the protocol and the browser.  You should
      // *always* verify the connection's origin and decide whether or not
      // to accept it.
      autoAcceptConnections: false,
    });

    // function originIsAllowed(origin) {
    //   // put logic here to detect whether the specified origin is allowed.
    //   return true;
    // }

    wsServer.on("request", function (request) {
      // if (!originIsAllowed(request.origin)) {
      //   // Make sure we only accept requests from an allowed origin
      //   request.reject();
      //   console.log((new Date()) + ' Connection from origin ' + request.origin + ' rejected.');
      //   return;
      // }

      var connection = request.accept("echo-protocol", request.origin);
      console.log(new Date() + " Connection accepted.");
      // console.log(connection);
      // webSocketClients.push({
      //   id_user:
      // })
      webSocketClients.push(connection);

      connection.on("message", function (message) {
        console.log(message);
        if (message.type === "utf8") {
          console.log("Received Message: " + message.utf8Data);
          connection.sendUTF(message.utf8Data);
        } else if (message.type === "binary") {
          console.log(
            "Received Binary Message of " + message.binaryData.length + " bytes"
          );
          connection.sendBytes(message.binaryData);
        }
      });
      connection.on("close", function (reasonCode, description) {
        console.log(
          new Date() + " Peer " + connection.remoteAddress + " disconnected."
        );
      });
    });

    // --- REST API
    const REST_API_PORT = 8002;

    const express = require("express");
    const bodyParser = require("body-parser");
    const boom = require("express-boom");

    const restApi = express();

    restApi.use(bodyParser.json());
    restApi.use(boom());

    restApi.get("/", async (httpRequest, httpResponse) => {
      httpResponse.json({});
    });

    restApi.get("/users", async (httpRequest, httpResponse) => {
      const [users] = await relationalDatabaseConnection.execute(
        `SELECT * from user`
      );
      return httpResponse.json(users);
    });

    restApi.get("/users/:id_user", async (httpRequest, httpResponse) => {
      const { id_user: idUser } = httpRequest.params;
      const [user] = await relationalDatabaseConnection.execute(
        `SELECT * from user where id_user = ${idUser}`
      );
      return httpResponse.json(user);
    });

    restApi.post("/users", async (httpRequest, httpResponse) => {
      const { name, age } = httpRequest.body;

      if (!name) {
        return httpResponse.boom.badRequest("A new user must have a name");
      }

      const [{ insertId: newUserId }] =
        await relationalDatabaseConnection.execute(
          `INSERT INTO user (name, age) VALUES ('${name}', ${age ? age : null})`
        );

      const [user] = await relationalDatabaseConnection.execute(
        `SELECT * from user where id_user = ${newUserId}`
      );

      return httpResponse.json(user);
    });

    restApi.listen(REST_API_PORT, async () => {
      console.log(`REST API listening on port ${REST_API_PORT}`);
    });

    // ------- Entire server
  } catch (error) {
    console.log(error);
  }
})();

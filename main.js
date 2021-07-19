// ----- Imports

// InfluxDB
const {
  InfluxDB,
  DEFAULT_RetryDelayStrategyOptions,
} = require("@influxdata/influxdb-client");
const { Point } = require("@influxdata/influxdb-client");
// MySQL
const mysql = require("mysql2/promise");

// ---- Definitions
const INFLUX_DB_TOKEN =
  "f3hMIrt80eERkkvLclrJAH7rTPW5ZXhbo0LYGsXKJqzQUYvR62QlyEG-_Ywx5-uHOynFeTq5gKksoNeJmnghXg==";
const INFLUX_DB_URL = "https://us-west-2-1.aws.cloud2.influxdata.com";
const INFLUX_DB_ORGANIZATION = "lpcluizhenrique@gmail.com";
const INFLUX_DB_BUCKET = `emg_signals`;
const INFLUX_DB_HOST = "host1";

const MYSQL_HOSTNAME = "sql10.freemysqlhosting.net";
const MYSQL_USERNAME = "sql10426279";
const MYSQL_PASSWORD = "2b2YjcvARE";
const MYSQL_DATABASE = "sql10426279";

let relationalDatabaseConnection;

const webSocketClients = [];

function isStringJson(str) {
  try {
    JSON.parse(str);
  } catch (e) {
    return false;
  }
  return true;
}

async function storeSignalSample(idUser, signalType, signalLevel) {
  try {
    const client = new InfluxDB({
      url: INFLUX_DB_URL,
      token: INFLUX_DB_TOKEN,
    });

    const writeApi = client.getWriteApi(
      INFLUX_DB_ORGANIZATION,
      INFLUX_DB_BUCKET
    );
    writeApi.useDefaultTags({ host: INFLUX_DB_HOST });
    const point = new Point(signalType)
      .floatField("signal_level", signalLevel)
      .timestamp(new Date())
      .tag("id_user", idUser);

    writeApi.writePoint(point);

    await writeApi.close();
    console.log("InfluxDB storage FINISHED");
  } catch (error) {
    console.error(error);
    console.log("\\nFinished ERROR");
  }
}

async function processSignal(signal) {
  const { idUser, signalSamples } = JSON.parse(signal);

  console.log("Received signal via MQTT: ", JSON.parse(signal));

  for (const signalSample of signalSamples) {
    await storeSignalSample(idUser, "emg", signalSample);
  }
  const [[{ userExists }]] = await relationalDatabaseConnection.execute(
    `SELECT count(*) as userExists  from user where id_user = ${idUser}`
  );

  if (userExists !== 1) return 0;

  if (webSocketClients.length > 0) {
    webSocketClients[0].sendUTF(signalSamples);
    console.log("Transmitted to websocket");
  }
}

//---------------------------------------------------- MQTT Subscriber Code
const mqtt = require("mqtt");
const exp = require("constants");
const { query } = require("express");
(async () => {
  try {
    relationalDatabaseConnection = await mysql.createConnection({
      host: MYSQL_HOSTNAME,
      user: MYSQL_USERNAME,
      password: MYSQL_PASSWORD,
      database: MYSQL_DATABASE,
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
        if (!isStringJson(message.toString())) return;
        await processSignal(message);
        return;
      }
      console.log("No handler for topic %s", topic);
    });
    // ----------------------
    // ----------------------
    // ---------------------- WebSocket

    const WebSocketServer = require("websocket").server;
    const http = require("http");

    const server = http.createServer(function (request, response) {
      console.log(new Date() + " Received request for " + request.url);
      response.writeHead(404);
      response.end();
    });
    server.listen(8000, function () {
      console.log(new Date() + " Server is listening on port 8000");
    });

    wsServer = new WebSocketServer({
      httpServer: server,
      autoAcceptConnections: false,
    });

    wsServer.on("request", function (request) {
      const connection = request.accept("echo-protocol", request.origin);
      console.log(new Date() + " Connection accepted.");
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

    restApi.get(
      "/users/:id_user/samples",
      async (httpRequest, httpResponse) => {
        const { id_user: idUser } = httpRequest.params;
        const { from, to } = httpRequest.query;

        const queryApi = new InfluxDB({
          url: INFLUX_DB_URL,
          token: INFLUX_DB_TOKEN,
        }).getQueryApi(INFLUX_DB_ORGANIZATION);

        const fluxQuery = `from(bucket: "emg_signals")
            |> range(start: ${from}, stop: ${to})
            |> filter(fn: (r) => r["_measurement"] == "emg")
            |> filter(fn: (r) => r["_field"] == "signal_level")
            |> filter(fn: (r) => r["host"] == "${INFLUX_DB_HOST}")
            |> filter(fn: (r) => r["id_user"] == "${idUser}")`;

        console.log("*** QUERY to InfluxDB was made ***");
        const samples = [];
        queryApi.queryRows(fluxQuery, {
          next(row, tableMeta) {
            const rowObject = tableMeta.toObject(row);
            samples.push({
              time: rowObject._time,
              signalLevel: rowObject._value,
            });
          },
          error(error) {
            console.error(error);
            console.log("\nFinished InfluxDB Query with ERROR");
          },
          complete() {
            console.log("\nFinished InfluxDB Query with SUCCESS");
            return httpResponse.json(samples);
          },
        });
      }
    );

    restApi.listen(REST_API_PORT, async () => {
      console.log(`REST API listening on port ${REST_API_PORT}`);
    });

    // ------- Entire server
  } catch (error) {
    console.log(error);
  }
})();

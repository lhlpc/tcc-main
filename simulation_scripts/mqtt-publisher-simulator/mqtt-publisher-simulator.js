const mqtt = require("mqtt");
const client = mqtt.connect("mqtt://broker.hivemq.com");

const data = {
  signalSamples: [1, 2, 3.4],
  idUser: 2,
};

client.publish("lpctcc/emg", JSON.stringify(data));
console.log("enviado");

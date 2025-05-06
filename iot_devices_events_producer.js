const { Kafka } = require('kafkajs');
const { SchemaRegistry, SchemaType } = require('@kafkajs/confluent-schema-registry');
const dotenv = require('dotenv');
dotenv.config();

const schemaRegistryUrl = process.env.SCHEMA_REGISTRY_URL;
const schemaRegistryUser = process.env.SCHEMA_REGISTRY_API_KEY;
const schemaRegistryPass = process.env.SCHEMA_REGISTRY_API_SECRET;

const kafka = new Kafka({
  clientId: process.env.KAFKA_CLIENT_ID,
  brokers: [process.env.KAFKA_BROKER],
  ssl: true,
  sasl: {
    mechanism: 'plain',
    username: process.env.KAFKA_USERNAME,
    password: process.env.KAFKA_PASSWORD
  }
});

const registry = new SchemaRegistry({
  host: schemaRegistryUrl,
  auth: {
    username: schemaRegistryUser,
    password: schemaRegistryPass
  }
});

const sensorSchema = {
  type: 'record',
  name: 'SensorData',
  namespace: 'com.example',
  fields: [
    { name: 'sensorId', type: 'string' },
    { name: 'timestamp', type: 'long' },
    { name: 'temperature', type: 'float' },
    { name: 'humidity', type: 'float' },
    { name: 'battery', type: 'float' },
    { name: 'airQuality', type: 'float' }
  ]
};

const topic = 'sensor-data';

// Sensor configuration: each sensor has its own expected normal range and margin for anomalies.
const sensorConfig = {
  'sensor-1': {
    temperature: { min: 18, max: 25, margin: 5 },
    humidity: { min: 40, max: 60, margin: 10 },
    battery: { min: 50, max: 100, margin: 20 },
    airQuality: { min: 0,  max: 50, margin: 20 }
  },
  'sensor-2': {
    temperature: { min: 10, max: 20, margin: 5 },
    humidity: { min: 30, max: 50, margin: 10 },
    battery: { min: 60, max: 100, margin: 20 },
    airQuality: { min: 5,  max: 60, margin: 20 }
  },
  'sensor-3': {
    temperature: { min: 20, max: 30, margin: 5 },
    humidity: { min: 35, max: 65, margin: 10 },
    battery: { min: 40, max: 90, margin: 20 },
    airQuality: { min: 10, max: 70, margin: 20 }
  },
  'sensor-4': {
    temperature: { min: 15, max: 22, margin: 5 },
    humidity: { min: 45, max: 70, margin: 10 },
    battery: { min: 30, max: 100, margin: 20 },
    airQuality: { min: 0,  max: 40, margin: 20 }
  },
  'sensor-5': {
    temperature: { min: 16, max: 28, margin: 5 },
    humidity: { min: 40, max: 65, margin: 10 },
    battery: { min: 50, max: 100, margin: 20 },
    airQuality: { min: 15, max: 75, margin: 20 }
  }
};

// The sensor state stores for each measurement both a normal (last known good) value 
// and an optional persistent anomaly state.
const sensorStates = {};

// Initialize state for a sensor using the midpoint of each expected range.
function initSensorState(sensorId, config) {
  sensorStates[sensorId] = {};
  Object.keys(config).forEach((measurement) => {
    const mConfig = config[measurement];
    const mid = (mConfig.min + mConfig.max) / 2;
    sensorStates[sensorId][measurement] = { normal: mid, anomaly: null };
  });
}

// A helper to update the persistent anomaly value with a small random walk,
// ensuring it remains outside the normal range.
function updatePersistentAnomalyValue(current, config, isBelow) {
  const delta = config.margin * 0.05;
  let newValue = current + (Math.random() * 2 - 1) * delta;
  if (isBelow) {
    if (newValue >= config.min) newValue = config.min - 0.1;
  } else {
    if (newValue <= config.max) newValue = config.max + 0.1;
  }
  return newValue;
}

// Generate the next "normal" reading by applying a small random walk while clamping within range.
function getNextNormalValue(oldValue, config) {
  const range = config.max - config.min;
  const delta = range * 0.05;
  let newValue = oldValue + (Math.random() * 2 - 1) * delta;
  return Math.max(config.min, Math.min(newValue, config.max));
}

// Generate a one-off transient anomaly value based on the current normal value.
function getAnomalyValue(normalValue, config) {
  return Math.random() < 0.5 
    ? normalValue - config.margin * (1 + Math.random())
    : normalValue + config.margin * (1 + Math.random());
}

// Update a sensor measurement value based on its current state and configuration.
// If a persistent anomaly is active (and has not expired), update it;
// otherwise, decide on generating a transient or persistent anomaly or a normal value.
function updateMeasurement(stateEntry, config) {
  const now = Date.now();

  // If in persistent anomaly mode and not expired, update its value with a small random walk.
  if (stateEntry.anomaly && now < stateEntry.anomaly.endTime) {
    stateEntry.anomaly.value = updatePersistentAnomalyValue(stateEntry.anomaly.value, config, stateEntry.anomaly.isBelow);
    return stateEntry.anomaly.value;
  }
  // Clear expired persistent anomaly.
  if (stateEntry.anomaly && now >= stateEntry.anomaly.endTime) {
    stateEntry.anomaly = null;
  }
  
  // Decide whether to produce an anomaly (10% chance).
  const anomalyProbability = 0.1;
  if (Math.random() < anomalyProbability) {
    const anomalyValue = getAnomalyValue(stateEntry.normal, config);
    // With 3% chance, lock the anomaly for 120 seconds.
    const persistentChance = 0.03;
    if (Math.random() < persistentChance) {
      const isBelow = anomalyValue < stateEntry.normal;
      stateEntry.anomaly = {
        value: anomalyValue,
        endTime: now + 3600 * 1000, // Persist anomaly for 600 seconds.
        isBelow: isBelow
      };
      return stateEntry.anomaly.value;
    } else {
      // Transient anomaly: return anomaly value without altering the stored normal state.
      return anomalyValue;
    }
  } else {
    // Normal behavior: update the sensor's normal state with a small random walk.
    stateEntry.normal = getNextNormalValue(stateEntry.normal, config);
    return stateEntry.normal;
  }
}

async function run() {
  const { id: schemaId } = await registry.register({
    type: SchemaType.AVRO,
    schema: JSON.stringify(sensorSchema)
  });
  console.log('Schema registered with ID:', schemaId);

  const producer = kafka.producer();
  await producer.connect();
  console.log('Producer connected. Starting data simulation...');

  // Simulate and send sensor data every second.
  setInterval(async () => {
    // Choose a sensor from our configuration.
    const sensors = Object.keys(sensorConfig);
    const sensorId = sensors[Math.floor(Math.random() * sensors.length)];

    // Initialize sensor state if not already done.
    if (!sensorStates[sensorId]) {
      initSensorState(sensorId, sensorConfig[sensorId]);
    }

    // 10% chance to send an empty sensorId to simulate an irregularity.
    const includeSensorId = Math.random() > 0.1;
    const finalSensorId = includeSensorId ? sensorId : '';

    // Timestamp: current time normally, with a 10% chance to introduce drift (up to 5 minutes).
    let timestamp = Date.now();
    if (Math.random() < 0.1) {
      const drift = (Math.random() < 0.5 ? -1 : 1) * Math.floor(Math.random() * 5 * 60 * 1000);
      timestamp += drift;
    }

    const config = sensorConfig[sensorId];
    const state = sensorStates[sensorId];

    // Update each measurement.
    const temperature = updateMeasurement(state.temperature, config.temperature);
    const humidity = updateMeasurement(state.humidity, config.humidity);
    const battery = updateMeasurement(state.battery, config.battery);
    const airQuality = updateMeasurement(state.airQuality, config.airQuality);

    // Construct the sensor data payload.
    const sensorData = {
      sensorId: finalSensorId,
      timestamp,
      temperature,
      humidity,
      battery,
      airQuality
    };

    try {
      const encodedMessage = await registry.encode(schemaId, sensorData);
      await producer.send({
        topic,
        messages: [{ value: encodedMessage }]
      });
      console.log('Sent message:', sensorData);
    } catch (error) {
      console.error('Error encoding/sending message:', error);
    }

    // 5% chance to send a duplicate message.
    if (Math.random() < 0.05) {
      try {
        const encodedMessageDup = await registry.encode(schemaId, sensorData);
        await producer.send({
          topic,
          messages: [{ value: encodedMessageDup }]
        });
        console.log('Sent duplicate message:', sensorData);
      } catch (error) {
        console.error('Error encoding/sending duplicate message:', error);
      }
    }
  }, 1000);
}

run().catch(e => {
  console.error('Error running producer:', e);
});

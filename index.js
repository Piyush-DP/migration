//Module to Connect to MongoDb
const { MongoClient, ObjectId } = require("mongodb");
const { createClient } = require("@clickhouse/client");
const { v4: uuidv4 } = require("uuid");
require("dotenv").config();

const twilio_authToken = process.env.twillioauth;
const twilio_sid = process.env.twilliosid;
const twilioClient = require("twilio")(twilio_sid, twilio_authToken);

(async () => {
  const uri = process.env.mongodburi;
  const dbName = "IOTData";
  const chConn = createClient({
    database: process.env.chdb,
    url: process.env.churl,
    password: process.env.chpass,
  });
  // Create a new MongoClient
  const client = new MongoClient(uri, {
    useUnifiedTopology: true,
  });
  try {
    await client.connect();
    const db = client.db(dbName);
    const collections = (await db.listCollections().toArray()).filter(
      (collection) =>
        !collection.name.startsWith("system.") &&
        !(
          collection.name === "Stores" ||
          collection.name === "workspaceFormulas"
        )
    );
    if (collections.length === 0) {
      console.log("no collection found");
      return;
    }
    const start = new Date("1970-11-10T20:00:00.000Z"); // Start of yesterday
    const end = new Date("2024-11-15T23:15:00.000Z");
    for (let collection of collections) {
      await processCollection(collection, db, start, end, chConn);
    }
    const msz = await twilioClient.messages.create({
      body: `Data migration complete`,
      to: `+91${process.env.phoneno}`,
      from: `+19377447586`,
    });
    console.log(`Data migration complete`);
  } catch (err) {
    console.log(err);
  } finally {
    await client.close();
  }
})();
async function processCollection(collection, db, start, end, chConn) {
  const batchSize = 100000;
  let skip = 0;
  while (true) {
    const documents = await db
      .collection(collection.name)
      .find({
        calculated_at: {
          $gte: start,
          $lte: end,
        },
      })
      .skip(skip)
      .limit(batchSize)
      .toArray();
    if (documents.length === 0) {
      console.log(`Completed migration for collection :${collection.name}`);
      await twilioClient.messages.create({
        body: `Completed migration for collection :${collection.name}`,
        to: `+91${process.env.phoneno}`,
        from: `+19377447586`,
      });
      break;
    }
    const newIotDataList = documents.map((doc) => {
      const { calculated_at, metadata, boolData, floatData } = doc;
      return {
        _id: uuidv4(),
        calculated_at: new Date(
          new Date(calculated_at).toUTCString()
        ).getTime(),
        value: boolData ? 1 : floatData || 0,
        sensorId: metadata?.sensorId?.toString() || null,
        sensorTag: metadata?.sensorTag?.toString() || null,
        storeId: metadata?.storeId?.toString() || null,
        remarkId: metadata?.remarkId?.toString() || null,
        assetId: collection?.name.toString() || null,
        unit: metadata?.unit?.toString() || null,
        version: metadata?.version || null,
        valueType: "boolData" in doc ? "BOOLEAN" : "FLOAT",
      };
    });
    chConn.insert({
      table: "iot_sensors_data_distributed",
      format: "JSONEachRow",
      values: newIotDataList,
    });
    console.log(
      `Migrated ${documents.length} records from collection: ${
        collection.name
      }, batch: ${skip / batchSize + 1}`
    );
    skip += batchSize;
  }
}

import test from "ava";
import makeMongoDBStore from "../src/store";
import { JourneyCommittedEvent, MongoDBModel } from "../src/types";

test("it should not apply an insertOne twice", async (t) => {
  interface TestCollection {
    id: number;
    name: string;
  }

  const model: MongoDBModel<TestCollection> = {
    name: "model_2",
    version: "1",
    transform(event: JourneyCommittedEvent) {
      return [
        {
          insertOne: { id: event.id, name: "test_" + event.type },
        },
      ];
    },
  };

  const store = await makeMongoDBStore({
    name: "optimistic_locking",
    dbName: "mongodb_store_driver_v4_optimistic",
    url: "mongodb://localhost:27017",
    models: [model],
  });

  await store.clean();

  await store.handleEvents([
    {
      id: 1,
      type: "event_1",
      payload: {},
    },
    {
      id: 2,
      type: "event_2",
      payload: {},
    },
    {
      id: 2,
      type: "event_2",
      payload: {},
    },
    {
      id: 2,
      type: "event_2",
      payload: {},
    },
    {
      id: 3,
      type: "event_3",
      payload: {},
    },
  ]);

  const collection = store.getDriver(model);
  const result = await collection.find().toArray();
  t.log(result);
  t.is(result.length, 3);
});

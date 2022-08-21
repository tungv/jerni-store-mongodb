import test from "ava";
import readPipeline from "../../src/read";
import makeMongoDBStore from "../../src/store";
import { JourneyCommittedEvent, MongoDBModel } from "../../src/types";

interface TestCollection {
  id: number;
  name: string;
}

test("it should allow reading data from the same collection", async (t) => {
  t.plan(2);

  const model_1: MongoDBModel<TestCollection> = {
    name: "model_read_1",
    version: "1",
    transform(event: JourneyCommittedEvent) {
      if (event.type === "init") {
        return [
          {
            insertMany: [
              { id: 1, name: "test-model-1--item-1" },
              { id: 2, name: "test-model-1--item-2" },
              { id: 3, name: "test-model-1--item-3" },
            ],
          },
        ];
      }

      if (event.type === "test") {
        const res = readPipeline([
          { $match: { id: 2 } },
          { $project: { name: 1 } },
        ]);

        t.deepEqual(res[0].name, "test-model-1--item-2");
        return [];
      }
    },
  };

  const model_2: MongoDBModel<TestCollection> = {
    name: "model_read_2",
    version: "1",
    transform(event: JourneyCommittedEvent) {
      if (event.type === "init") {
        return [
          {
            insertMany: [
              { id: 1, name: "test-model-2--item-1" },
              { id: 2, name: "test-model-2--item-2" },
              { id: 3, name: "test-model-2--item-3" },
            ],
          },
        ];
      }

      if (event.type === "test") {
        const res = readPipeline([
          { $match: { id: 2 } },
          { $project: { name: 1 } },
        ]);

        t.deepEqual(res[0].name, "test-model-2--item-2");
        return [];
      }
    },
  };

  const store = await makeMongoDBStore({
    name: "test_register_models",
    dbName: "mongodb_store_driver_v4_test_read_pipeline",
    url: "mongodb://localhost:27017",
    models: [model_1, model_2],
  });

  await store.clean();
  await store.handleEvents([
    {
      id: 1,
      type: "init",
      payload: {},
    },
    {
      id: 2,
      type: "test",
      payload: {},
    },
  ]);
});

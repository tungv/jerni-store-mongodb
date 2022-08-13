import { AnyBulkWriteOperation, Collection, MongoClient } from "mongodb";
import {
  JourneyCommittedEvent,
  MongoDBStoreConfig,
  MongoDBStore,
  MongoDBModel,
  MongoOps,
} from "./types";

export default async function makeMongoDBStore(
  config: MongoDBStoreConfig,
): Promise<MongoDBStore> {
  const client = await MongoClient.connect(config.url);
  const db = client.db(config.dbName);

  const models = config.models;

  const store: MongoDBStore = {
    name: config.name,
    meta: {
      includes: [],
    },
    registerModels,
    getDriver,
    handleEvents,
    getLastSeenId,
    clean,
    dispose,
  };

  return store;

  function registerModels(map: Map<any, MongoDBStore>) {
    let includes = new Set<string>();
    let includesAll = false;

    models.forEach((model) => {
      map.set(model, store);

      // handle meta.includes
      const modelSpecificMeta = model.meta || model.transform.meta;
      if (
        !modelSpecificMeta ||
        !modelSpecificMeta.includes ||
        modelSpecificMeta.includes.length === 0
      ) {
        includesAll = true;
        return;
      }

      modelSpecificMeta.includes.forEach((type) => includes.add(type));
    });

    if (includesAll) {
      store.meta.includes = [];
    } else {
      store.meta.includes = [...Array.from(includes)].sort((a, z) =>
        a.localeCompare(z),
      );
    }
  }

  function getDriver<T>(model: MongoDBModel<T>): Collection<T> {
    return db.collection(getCollectionName(model));
  }

  async function handleEvents(events: JourneyCommittedEvent[]) {
    // TODO
    const outputs = events.map((event) => {
      return models.map((model) => model.transform(event));
    });

    let eventIndex = 0;

    for (const allChangesForAnEvent of outputs) {
      let modelIndex = 0;
      for (const changesForAModel of allChangesForAnEvent) {
        if (changesForAModel.length === 0) {
          continue;
        }
        const model = models[modelIndex];
        const collection = getDriver(model);

        await collection.bulkWrite(
          convertModelOpsToMongoDbBulkWriteOp(changesForAModel),
        );

        modelIndex++;
      }
      eventIndex++;
    }
  }

  async function getLastSeenId() {
    return 0;
  }

  async function clean() {
    // delete mongodb collections
    for (const model of models) {
      try {
        const collection = getDriver(model);
        await collection.drop();
      } catch (ex) {
        // ignore
      }
    }
  }

  async function dispose() {
    // close connections
    await client.close();
  }
}

function convertModelOpsToMongoDbBulkWriteOp<DocumentType>(
  ops: MongoOps<DocumentType>[],
): AnyBulkWriteOperation<DocumentType>[] {
  return ops.flatMap((op): AnyBulkWriteOperation<DocumentType>[] => {
    if ("insertOne" in op) {
      return [
        {
          insertOne: { document: op.insertOne },
        },
      ];
    }

    if ("insertMany" in op) {
      return op.insertMany.map((document) => ({
        insertOne: { document },
      }));
    }

    if ("updateOne" in op) {
      return [
        {
          updateOne: {
            filter: op.updateOne.where,
            update: op.updateOne.changes,
          },
        },
      ];
    }

    if ("updateMany" in op) {
      return [
        {
          updateMany: {
            filter: op.updateMany.where,
            update: op.updateMany.changes,
          },
        },
      ];
    }

    if ("deleteOne" in op) {
      return [
        {
          deleteOne: {
            filter: op.deleteOne.where,
          },
        },
      ];
    }

    if ("deleteMany" in op) {
      return [
        {
          deleteMany: {
            filter: op.deleteMany.where,
          },
        },
      ];
    }

    throw new Error(`Unknown op type: ${JSON.stringify(op)}`);
  });
}

function getCollectionName(model: MongoDBModel<any>) {
  return `${model.name}_v${model.version}`;
}

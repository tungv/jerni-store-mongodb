import { Collection, Document, MongoClient } from "mongodb";
import getCollectionName from "./getCollectionName";
import MongoDBModel from "./model";
import getBulkOperations from "./optimistic/getBulkOperations";
import { clearModelSlots, runWithModel, Signal } from "./read";
import {
  JourneyCommittedEvent,
  MongoDBStoreConfig,
  MongoDBStore,
} from "./types";

interface SnapshotDocument {
  __v: number;
  full_collection_name: string;
}

export default async function makeMongoDBStore(
  config: MongoDBStoreConfig,
): Promise<MongoDBStore> {
  const client = await MongoClient.connect(config.url);
  const db = client.db(config.dbName);

  const models = config.models;

  const snapshotCollection = db.collection<SnapshotDocument>("jerni__snapshot");

  // ensure snapshot collection
  for (const model of models) {
    const fullCollectionName = getCollectionName(model);
    await snapshotCollection.updateOne(
      {
        full_collection_name: fullCollectionName,
      },
      {
        $setOnInsert: {
          __v: 0,
          full_collection_name: fullCollectionName,
        },
      },
      {
        upsert: true,
      },
    );
  }

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

  function getDriver<T extends Document>(
    model: MongoDBModel<T>,
  ): Collection<T> {
    return db.collection(getCollectionName(model));
  }

  async function handleEvents(events: JourneyCommittedEvent[]) {
    let interuptedIndex = -1;
    const signals: Signal<any>[] = [];

    const outputs = events.map((event, index) => {
      if (interuptedIndex !== -1) {
        return [];
      }

      const out = models.map((model) => {
        try {
          return runWithModel(model, event);
        } catch (error) {
          if (error instanceof Signal) {
            console.debug(
              "event id=%d reads. Stop and processing previous event (from %d to before %d)",
              event.id,
              events[0].id,
              events[index].id,
            );

            interuptedIndex = index;

            signals.push(error);
            return [];
          }

          throw error;
        }
      });

      if (signals.length === 0) {
        // console.log("event id=%d clear. Continue", event.id);
        clearModelSlots();
      }

      return out;
    });

    let eventIndex = 0;

    for (const allChangesForAnEvent of outputs) {
      let modelIndex = 0;
      for (const changesForAModel of allChangesForAnEvent) {
        let __op = 0;
        const model = models[modelIndex];
        modelIndex++;
        if (changesForAModel === undefined || changesForAModel.length === 0) {
          continue;
        }

        const changesWithOp = changesForAModel.map((change) => {
          return {
            change,
            __op: __op++,
            __v: events[eventIndex].id,
          };
        });
        const collection = getDriver(model);

        const bulkWriteOperations = getBulkOperations(changesWithOp);

        await collection.bulkWrite(bulkWriteOperations);
      }
      eventIndex++;
    }

    // update snapshot collections
    const lastSeenId = events[events.length - 1].id;
    await snapshotCollection.updateMany(
      {
        full_collection_name: { $in: models.map(getCollectionName) },
      },
      {
        $set: {
          __v: lastSeenId,
        },
      },
    );

    // continue with remaining events
    if (interuptedIndex !== -1) {
      console.debug(
        "priming data for these %d event(s):\n%s",
        events.slice(interuptedIndex).length,
        require("util").inspect(signals, { depth: null, colors: true }),
      );

      // execute signals
      for (const signal of signals) {
        await signal.execute(db);
      }

      await handleEvents(events.slice(interuptedIndex));
    }
  }

  async function getLastSeenId() {
    const snapshotCollection =
      db.collection<SnapshotDocument>("jerni__snapshot");

    const registeredModels = await snapshotCollection
      .find({
        full_collection_name: { $in: models.map(getCollectionName) },
      })
      .toArray();

    if (registeredModels.length === 0) return 0;

    return Math.max(0, Math.min(...registeredModels.map((doc) => doc.__v)));
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

    // delete rows in snapshot collection
    await snapshotCollection.updateMany(
      {
        full_collection_name: { $in: models.map(getCollectionName) },
      },
      {
        $set: {
          __v: 0,
        },
      },
    );
  }

  async function dispose() {
    // close connections
    await client.close();
  }
}

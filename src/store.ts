import { Collection, MongoClient } from "mongodb";
import {
  JourneyCommittedEvent,
  MongoDBStoreConfig,
  MongoDBStore,
  MongoDBModel,
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
    return db.collection(model.name);
  }

  function handleEvents(events: JourneyCommittedEvent[]) {
    // TODO
  }

  async function getLastSeenId() {
    return 0;
  }

  async function clean() {
    // delete mongodb collections
  }

  async function dispose() {
    // close connections
    await client.close();
  }
}

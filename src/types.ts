import { Collection } from "mongodb";

export interface MongoDBStoreConfig {
  name: string;
  dbName: string;
  url: string;

  models: MongoDBModel<any>[];
}

export interface TransformFn {
  (events: JourneyCommittedEvent[]): any;
  meta?: StoreMeta;
}

export interface MongoDBModel<T> {
  name: string;
  version: string;
  transform: TransformFn;
  meta?: StoreMeta;
}

export interface StoreMeta {
  includes: string[];
}

export interface MongoDBStore {
  name: string;
  meta: StoreMeta;

  /**
   * journey instance will call this method to register models.
   * Registration of a model will tell which store this model belongs to.
   *
   * This is useful when journey.getReader() is called.
   */
  registerModels: (
    map: Map<
      {
        name: string;
        version: string;
      },
      MongoDBStore
    >,
  ) => void;

  getDriver<T>(model: MongoDBModel<T>): Collection<T>;
  handleEvents: (events: JourneyCommittedEvent[]) => void;
  getLastSeenId: () => Promise<number>;
  toString(): string;

  // listen
  clean: () => Promise<void>;
  dispose: () => Promise<void>;
}

export interface JourneyCommittedEvent {
  id: number;
  type: string;
  payload: unknown;
}

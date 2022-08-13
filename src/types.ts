import {
  Collection,
  DeleteManyModel,
  DeleteOneModel,
  InsertOneModel,
  UpdateManyModel,
  UpdateOneModel,
  WithId,
} from "mongodb";

export interface MongoDBStoreConfig {
  name: string;
  dbName: string;
  url: string;

  models: MongoDBModel<any>[];
}

export interface TransformFn<DocumentType> {
  (event: JourneyCommittedEvent): MongoOps<DocumentType>[];
  meta?: StoreMeta;
}

export interface MongoDBModel<DocumentType> {
  name: string;
  version: string;
  transform: TransformFn<DocumentType>;
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
  handleEvents: (events: JourneyCommittedEvent[]) => Promise<void>;
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

interface InsertOneOp<DocumentType> {
  insertOne: InsertOneModel<DocumentType>["document"];
}

interface InsertManyOp<DocumentType> {
  insertMany: InsertOneModel<DocumentType>["document"][];
}

interface UpdateOneOp<DocumentType> {
  updateOne: {
    where: UpdateOneModel<DocumentType>["filter"];
    changes: UpdateOneModel<DocumentType>["update"];
  };
}

interface UpdateManyOp<DocumentType> {
  updateMany: {
    where: UpdateManyModel<DocumentType>["filter"];
    changes: UpdateManyModel<DocumentType>["update"];
  };
}

interface DeleteOneOp<DocumentType> {
  deleteOne: {
    where: DeleteOneModel<DocumentType>["filter"];
  };
}

interface DeleteManyOp<DocumentType> {
  deleteMany: {
    where: DeleteManyModel<DocumentType>["filter"];
  };
}

export type MongoOps<DocumentType> =
  | InsertOneOp<DocumentType>
  | InsertManyOp<DocumentType>
  | UpdateOneOp<DocumentType>
  | UpdateManyOp<DocumentType>
  | DeleteOneOp<DocumentType>
  | DeleteManyOp<DocumentType>;

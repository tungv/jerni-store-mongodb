import { MongoDBModel } from "./types";

export default function getCollectionName(model: MongoDBModel<any>) {
  return `${model.name}_v${model.version}`;
}

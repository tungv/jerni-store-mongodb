import { Db, Document, UpdateFilter } from "mongodb";
import getCollectionName from "./getCollectionName";
import { JourneyCommittedEvent, MongoDBModel, TransformFn } from "./types";

let currentModel: MongoDBModel<any> | null = null;

export class Signal<DocumentType> extends Error {
  private model: MongoDBModel<DocumentType>;
  private pipeline: UpdateFilter<DocumentType>[];
  private slotIndex: number;

  constructor(
    model: MongoDBModel<DocumentType>,
    pipeline: UpdateFilter<DocumentType>[],
    slotIndex: number,
  ) {
    super("Signal");

    this.model = model;
    this.pipeline = pipeline;
    this.slotIndex = slotIndex;
  }

  async execute(db: Db) {
    const collectionName = getCollectionName(this.model);
    const collection = db.collection<DocumentType>(collectionName);

    const res = await collection.aggregate(this.pipeline).toArray();

    // write to the slot
    const slots = modelSlotsMap.get(this.model);
    slots![this.slotIndex] = res;

    return res;
  }
}

const modelSlotsMap = new Map<MongoDBModel<any>, Document[] | null>();
let currentModelSlotIndex = 0;

export function runWithModel<DocumentType>(
  model: MongoDBModel<DocumentType>,
  event: JourneyCommittedEvent,
) {
  currentModel = model;
  currentModelSlotIndex = 0;

  const res = model.transform(event);
  currentModel = null;
  return res;
}

export default function readPipeline<DocumentType>(
  pipeline: UpdateFilter<DocumentType>[],
) {
  console.log("readPipeline", pipeline);
  console.log("currentModel", currentModel);
  console.log("currentModelSlotIndex", currentModelSlotIndex);
  const model = currentModel as MongoDBModel<DocumentType>;

  const slots = modelSlotsMap.get(model);
  if (!slots) {
    const slot: Document[] = [];
    modelSlotsMap.set(model, slot);
    throw new Signal(model, pipeline, 0);
  }

  if (slots[currentModelSlotIndex] === null) {
    throw new Signal(model, pipeline, currentModelSlotIndex);
  }

  const res = slots[currentModelSlotIndex];
  currentModelSlotIndex++;
  return res;
}

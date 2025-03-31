import { MongoPersistence } from ".";

const createIndex = jest.fn();
const findOneAndUpdate = jest.fn();
const insertOne = jest.fn();
const findOne = jest.fn();
const persistence = new MongoPersistence("mongodb://localhost:27017");

jest.mock("mongodb", () => ({
  MongoClient: jest.fn().mockImplementation(() => ({
    db: jest.fn().mockImplementation(() => ({
      collection: jest.fn().mockImplementation(() => ({
        createIndex,
        findOneAndUpdate,
        insertOne,
        findOne,
      })),
    })),
  })),
}));

beforeEach(() => {
  createIndex.mockReset();
  findOneAndUpdate.mockReset();
  insertOne.mockReset();
  findOne.mockReset();
});

describe("init", () => {
  it("should create the indexes", async () => {
    await persistence.init();
    expect(createIndex).toHaveBeenNthCalledWith(1, { id: 1 }, { unique: true });
    expect(createIndex).toHaveBeenNthCalledWith(2, { status: 1 });
    expect(createIndex).toHaveBeenNthCalledWith(3, { status: 1, timeoutAt: 1 });
  });
});

describe("insert", () => {
  it("returns true if the workflow is inserted", async () => {
    const result = await persistence.insert(
      "workflow-1",
      "handler-1",
      "input-1",
    );

    expect(result).toBeTruthy();

    expect(insertOne).toHaveBeenCalledWith(
      expect.objectContaining({
        id: "workflow-1",
        handler: "handler-1",
        input: "input-1",
        status: "idle",
      }),
    );
  });

  it("returns false if the workflow already exists", async () => {
    insertOne.mockRejectedValue({ name: "MongoServerError", code: 11000 });

    const result = await persistence.insert(
      "workflow-1",
      "handler-1",
      "input-1",
    );

    expect(result).toBeFalsy();

    expect(insertOne).toHaveBeenCalledWith(
      expect.objectContaining({
        id: "workflow-1",
        handler: "handler-1",
        input: "input-1",
        status: "idle",
      }),
    );
  });

  it("fails if insertion fails", async () => {
    insertOne.mockRejectedValue(new Error("kapot"));
    const result = persistence.insert("workflow-1", "handler-1", "input-1");
    await expect(result).rejects.toThrow("kapot");

    expect(insertOne).toHaveBeenCalledWith(
      expect.objectContaining({
        id: "workflow-1",
        handler: "handler-1",
        input: "input-1",
        status: "idle",
      }),
    );
  });
});

describe("claim", () => {
  it("should return the workflow id if found", async () => {
    findOneAndUpdate.mockResolvedValue({ id: "workflow-1" });
    const now = new Date("2011-10-05T14:38:00.000Z");
    const timeoutAt = new Date("2011-10-05T14:48:00.000Z");
    const workflowId = await persistence.claim(now, timeoutAt);
    expect(workflowId).toEqual("workflow-1");

    expect(findOneAndUpdate).toHaveBeenCalledWith(
      {
        $or: [
          {
            status: "idle",
          },
          {
            status: { $in: ["running", "failed"] },
            timeoutAt: { $lt: now },
          },
        ],
      },
      {
        $set: {
          status: "running",
          timeoutAt,
        },
      },
      {
        projection: {
          _id: 0,
          id: 1,
        },
      },
    );
  });

  it("should return undefined if not found", async () => {
    const now = new Date("2011-10-05T14:38:00.000Z");
    const timeoutAt = new Date("2011-10-05T14:48:00.000Z");
    const workflowId = await persistence.claim(now, timeoutAt);
    expect(workflowId).toBeFalsy();

    expect(findOneAndUpdate).toHaveBeenCalledWith(
      {
        $or: [
          {
            status: "idle",
          },
          {
            status: { $in: ["running", "failed"] },
            timeoutAt: { $lt: now },
          },
        ],
      },
      {
        $set: {
          status: "running",
          timeoutAt,
        },
      },
      {
        projection: {
          _id: 0,
          id: 1,
        },
      },
    );
  });
});

describe("findOutput", () => {
  it("returns the output if found", async () => {
    findOne.mockResolvedValue({ steps: { "step-1": "output-1" } });
    const output = await persistence.findOutput("workflow-1", "step-1");
    expect(output).toEqual("output-1");

    expect(findOne).toHaveBeenCalledWith(
      {
        id: "workflow-1",
      },
      {
        projection: {
          _id: 0,
          "steps.step-1": 1,
        },
      },
    );
  });

  it("returns undefined if output is not found", async () => {
    const output = await persistence.findOutput("workflow-1", "step-1");
    expect(output).toBeUndefined();

    expect(findOne).toHaveBeenCalledWith(
      {
        id: "workflow-1",
      },
      {
        projection: {
          _id: 0,
          "steps.step-1": 1,
        },
      },
    );
  });
});

describe("findWakeUpAt", () => {
  it("returns the wakeUpAt if found", async () => {
    const wakeUpAt = new Date("2011-10-05T14:38:00.000Z");
    findOne.mockResolvedValue({ naps: { "nap-1": wakeUpAt } });
    const _wakeUpAt = await persistence.findWakeUpAt("workflow-1", "nap-1");
    expect(_wakeUpAt).toEqual(wakeUpAt);

    expect(findOne).toHaveBeenCalledWith(
      {
        id: "workflow-1",
      },
      {
        projection: {
          _id: 0,
          "naps.nap-1": 1,
        },
      },
    );
  });

  it("returns undefined if wakeUpAt is not found", async () => {
    const wakeUpAt = await persistence.findWakeUpAt("workflow-1", "nap-1");
    expect(wakeUpAt).toBeUndefined();

    expect(findOne).toHaveBeenCalledWith(
      {
        id: "workflow-1",
      },
      {
        projection: {
          _id: 0,
          "naps.nap-1": 1,
        },
      },
    );
  });
});

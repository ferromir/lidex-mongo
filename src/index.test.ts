import { makeMongoPersistence } from ".";

const createIndex = jest.fn();
const findOneAndUpdate = jest.fn();
const insertOne = jest.fn();
const findOne = jest.fn();
const updateOne = jest.fn();
const close = jest.fn();
const persistence = makeMongoPersistence("mongodb://localhost:27017");

jest.mock("mongodb", () => ({
  MongoClient: jest.fn().mockImplementation(() => ({
    db: jest.fn().mockImplementation(() => ({
      collection: jest.fn().mockImplementation(() => ({
        createIndex,
        findOneAndUpdate,
        insertOne,
        findOne,
        updateOne,
      })),
    })),
    close,
  })),
}));

beforeEach(() => {
  createIndex.mockReset();
  findOneAndUpdate.mockReset();
  insertOne.mockReset();
  findOne.mockReset();
  updateOne.mockReset();
  close.mockReset();
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

describe("findRunData", () => {
  it("returns run data if workflow is found", async () => {
    findOne.mockResolvedValue({
      handler: "handler-1",
      input: "input-1",
      failures: 1,
    });

    const data = await persistence.findRunData("workflow-1");

    expect(data).toEqual({
      handler: "handler-1",
      input: "input-1",
      failures: 1,
    });

    expect(findOne).toHaveBeenCalledWith(
      {
        id: "workflow-1",
      },
      {
        projection: {
          _id: 0,
          handler: 1,
          input: 1,
          failures: 1,
        },
      },
    );
  });

  it("returns undefined if workflow is not found", async () => {
    const data = await persistence.findRunData("workflow-1");
    expect(data).toBeUndefined();

    expect(findOne).toHaveBeenCalledWith(
      {
        id: "workflow-1",
      },
      {
        projection: {
          _id: 0,
          handler: 1,
          input: 1,
          failures: 1,
        },
      },
    );
  });
});

describe("setAsFinished", () => {
  it("updates workflow status", async () => {
    await persistence.setAsFinished("workflow-1");

    expect(updateOne).toHaveBeenCalledWith(
      {
        id: "workflow-1",
      },
      {
        $set: { status: "finished" },
      },
    );
  });
});

describe("findStatus", () => {
  it("returns workflow status if found", async () => {
    findOne.mockResolvedValue({ status: "running" });
    const status = await persistence.findStatus("workflow-1");
    expect(status).toEqual("running");
  });

  it("returns undefined if workflow not found", async () => {
    const status = await persistence.findStatus("workflow-1");
    expect(status).toBeUndefined();
  });
});

describe("updateStatus", () => {
  it("updates the workflow", async () => {
    const timeoutAt = new Date("2011-10-05T14:48:00.000Z");

    await persistence.updateStatus(
      "workflow-1",
      "failed",
      timeoutAt,
      1,
      "kapot",
    );

    expect(updateOne).toHaveBeenCalledWith(
      {
        id: "workflow-1",
      },
      {
        $set: {
          status: "failed",
          timeoutAt,
          failures: 1,
          lastError: "kapot",
        },
      },
    );
  });
});

describe("updateOutput", () => {
  it("updates the workflow", async () => {
    const timeoutAt = new Date("2011-10-05T14:48:00.000Z");

    await persistence.updateOutput(
      "workflow-1",
      "step-1",
      "output-1",
      timeoutAt,
    );

    expect(updateOne).toHaveBeenCalledWith(
      {
        id: "workflow-1",
      },
      {
        $set: {
          ["steps.step-1"]: "output-1",
          timeoutAt,
        },
      },
    );
  });
});

describe("updateWakeUpAt", () => {
  it("updates the workflow", async () => {
    const wakeUpAt = new Date("2011-10-05T14:47:00.000Z");
    const timeoutAt = new Date("2011-10-05T14:48:00.000Z");

    await persistence.updateWakeUpAt(
      "workflow-1",
      "nap-1",
      wakeUpAt,
      timeoutAt,
    );

    expect(updateOne).toHaveBeenCalledWith(
      {
        id: "workflow-1",
      },
      {
        $set: {
          ["naps.nap-1"]: wakeUpAt,
          timeoutAt,
        },
      },
    );
  });
});

describe("terminate", () => {
  it("should terminate the client", async () => {
    await persistence.terminate();
    expect(close).toHaveBeenCalled();
  });
});

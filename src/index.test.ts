import { MongoPersistence } from ".";

const createIndex = jest.fn();
const findOneAndUpdate = jest.fn();

jest.mock("mongodb", () => ({
  MongoClient: jest.fn().mockImplementation(() => ({
    db: jest.fn().mockImplementation(() => ({
      collection: jest.fn().mockImplementation(() => ({
        createIndex,
        findOneAndUpdate,
      })),
    })),
  })),
}));

describe("init", () => {
  it("should create the indexes", async () => {
    const persistence = new MongoPersistence("mongodb://localhost:27017");
    await persistence.init();
    expect(createIndex).toHaveBeenNthCalledWith(1, { id: 1 }, { unique: true });
    expect(createIndex).toHaveBeenNthCalledWith(2, { status: 1 });
    expect(createIndex).toHaveBeenNthCalledWith(3, { status: 1, timeoutAt: 1 });
  });
});

beforeEach(() => {
  createIndex.mockReset();
  findOneAndUpdate.mockReset();
});

describe("claim", () => {
  it("should return the workflow id if found", async () => {
    const persistence = new MongoPersistence("mongodb://localhost:27017");
    const timeoutAt = new Date("2011-10-05T14:48:00.000Z");
    findOneAndUpdate.mockResolvedValue({ id: "workflow-1" });
    const workflowId = await persistence.claim(timeoutAt);
    expect(workflowId).toEqual("workflow-1");

    expect(findOneAndUpdate).toHaveBeenCalledWith(
      {
        $or: [
          {
            status: "idle",
          },
          {
            status: { $in: ["running", "failed"] },
            timeoutAt: { $lt: timeoutAt },
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
      }
    );
  });

  it("should return undefined if not found", async () => {
    const persistence = new MongoPersistence("mongodb://localhost:27017");
    const timeoutAt = new Date("2011-10-05T14:48:00.000Z");
    const workflowId = await persistence.claim(timeoutAt);
    expect(workflowId).toBeFalsy();

    expect(findOneAndUpdate).toHaveBeenCalledWith(
      {
        $or: [
          {
            status: "idle",
          },
          {
            status: { $in: ["running", "failed"] },
            timeoutAt: { $lt: timeoutAt },
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
      }
    );
  });
});

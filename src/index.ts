import { MongoClient } from "mongodb";

type Status = "idle" | "running" | "failed" | "finished" | "aborted";

interface Workflow {
  id: string;
  handler: string;
  input: unknown;
  status: Status;
  timeoutAt?: Date;
  failures?: number;
  lastError?: string;
  steps?: { [key: string]: unknown };
  naps?: { [key: string]: Date };
}

type RunData = Pick<Workflow, "handler" | "input" | "failures">;

export function makeMongoPersistence(url: string) {
  const client = new MongoClient(url);
  const db = client.db();
  const workflows = db.collection("workflows");

  async function init() {
    await workflows.createIndex({ id: 1 }, { unique: true });
    await workflows.createIndex({ status: 1 });
    await workflows.createIndex({ status: 1, timeoutAt: 1 });
  }

  async function insert(
    workflowId: string,
    handler: string,
    input: unknown,
  ): Promise<boolean> {
    try {
      await workflows.insertOne({
        id: workflowId,
        handler,
        input,
        status: "idle",
      });

      return true;
    } catch (error) {
      const e = error as { name: string; code: number };

      // Workflow already started, ignore.
      if (e.name === "MongoServerError" && e.code === 11000) {
        return false;
      }

      throw error;
    }
  }

  async function claim(
    now: Date,
    timeoutAt: Date,
  ): Promise<string | undefined> {
    const workflow = await workflows.findOneAndUpdate(
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

    return workflow?.id;
  }

  async function findOutput(
    workflowId: string,
    stepId: string,
  ): Promise<unknown> {
    const workflow = await workflows.findOne(
      {
        id: workflowId,
      },
      {
        projection: {
          _id: 0,
          [`steps.${stepId}`]: 1,
        },
      },
    );

    if (workflow && workflow.steps) {
      return workflow.steps[stepId];
    }

    return undefined;
  }

  async function findWakeUpAt(
    workflowId: string,
    napId: string,
  ): Promise<Date | undefined> {
    const workflow = await workflows.findOne(
      {
        id: workflowId,
      },
      {
        projection: {
          _id: 0,
          [`naps.${napId}`]: 1,
        },
      },
    );

    if (workflow && workflow.naps) {
      return workflow.naps[napId];
    }

    return undefined;
  }

  async function findRunData(workflowId: string): Promise<RunData | undefined> {
    const workflow = await workflows.findOne(
      {
        id: workflowId,
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

    if (workflow) {
      return {
        handler: workflow.handler,
        input: workflow.input,
        failures: workflow.failures,
      };
    }

    return undefined;
  }

  async function setAsFinished(workflowId: string): Promise<void> {
    await workflows.updateOne(
      {
        id: workflowId,
      },
      {
        $set: { status: "finished" },
      },
    );
  }

  async function findStatus(workflowId: string): Promise<Status | undefined> {
    const workflow = await workflows.findOne(
      {
        id: workflowId,
      },
      {
        projection: {
          _id: 0,
          status: 1,
        },
      },
    );

    return workflow?.status;
  }

  async function updateStatus(
    workflowId: string,
    status: Status,
    timeoutAt: Date,
    failures: number,
    lastError: string,
  ): Promise<void> {
    await workflows.updateOne(
      {
        id: workflowId,
      },
      {
        $set: {
          status,
          timeoutAt,
          failures,
          lastError,
        },
      },
    );
  }

  async function updateOutput(
    workflowId: string,
    stepId: string,
    output: unknown,
    timeoutAt: Date,
  ): Promise<void> {
    await workflows.updateOne(
      {
        id: workflowId,
      },
      {
        $set: {
          [`steps.${stepId}`]: output,
          timeoutAt,
        },
      },
    );
  }

  async function updateWakeUpAt(
    workflowId: string,
    napId: string,
    wakeUpAt: Date,
    timeoutAt: Date,
  ): Promise<void> {
    await workflows.updateOne(
      {
        id: workflowId,
      },
      {
        $set: {
          [`naps.${napId}`]: wakeUpAt,
          timeoutAt,
        },
      },
    );
  }

  async function terminate() {
    await client.close();
  }

  return {
    init,
    insert,
    claim,
    findOutput,
    findWakeUpAt,
    findRunData,
    setAsFinished,
    findStatus,
    updateStatus,
    updateOutput,
    updateWakeUpAt,
    terminate,
  };
}

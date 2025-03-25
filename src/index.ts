import { Collection, MongoClient } from "mongodb";

type Status = "idle" | "running" | "failed" | "finished" | "aborted";

interface RunData {
  handler: string;
  input: unknown;
  failures?: number;
}

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

export class MongoPersistence {
  readonly workflows: Collection<Workflow>;

  constructor(url: string) {
    const client = new MongoClient(url);
    const db = client.db();
    this.workflows = db.collection("workflows");
  }

  async init() {
    await this.workflows.createIndex({ id: 1 }, { unique: true });
    await this.workflows.createIndex({ status: 1 });
    await this.workflows.createIndex({ status: 1, timeoutAt: 1 });
  }

  async insert(
    workflowId: string,
    handler: string,
    input: unknown
  ): Promise<boolean> {
    try {
      await this.workflows.insertOne({
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

  async claim(timeoutAt: Date): Promise<string | undefined> {
    const workflow = await this.workflows.findOneAndUpdate(
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

    return workflow?.id;
  }

  async findOutput(workflowId: string, stepId: string): Promise<unknown> {
    const workflow = await this.workflows.findOne(
      {
        id: workflowId,
      },
      {
        projection: {
          _id: 0,
          [`steps.${stepId}`]: 1,
        },
      }
    );

    if (workflow && workflow.steps) {
      return workflow.steps[stepId];
    }

    return undefined;
  }

  async findWakeUpAt(
    workflowId: string,
    napId: string
  ): Promise<Date | undefined> {
    const workflow = await this.workflows.findOne(
      {
        id: workflowId,
      },
      {
        projection: {
          _id: 0,
          [`naps.${napId}`]: 1,
        },
      }
    );

    if (workflow && workflow.naps) {
      return workflow.naps[napId];
    }

    return undefined;
  }

  async findRunData(workflowId: string): Promise<RunData | undefined> {
    const workflow = await this.workflows.findOne(
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
      }
    );

    if (workflow) {
      return workflow;
    }

    return undefined;
  }

  async setAsFinished(workflowId: string): Promise<void> {
    await this.workflows.updateOne(
      {
        id: workflowId,
      },
      {
        $set: { status: "finished" },
      }
    );
  }

  async findStatus(workflowId: string): Promise<Status | undefined> {
    const workflow = await this.workflows.findOne(
      {
        id: workflowId,
      },
      {
        projection: {
          _id: 0,
          status: 1,
        },
      }
    );

    return workflow?.status;
  }

  async updateStatus(
    workflowId: string,
    status: Status,
    timeoutAt: Date,
    failures: number,
    lastError: string
  ): Promise<void> {
    await this.workflows.updateOne(
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
      }
    );
  }

  async updateOutput(
    workflowId: string,
    stepId: string,
    output: unknown,
    timeoutAt: Date
  ): Promise<void> {
    await this.workflows.updateOne(
      {
        id: workflowId,
      },
      {
        $set: {
          [`steps.${stepId}`]: output,
          timeoutAt,
        },
      }
    );
  }

  async updateWakeUpAt(
    workflowId: string,
    napId: string,
    wakeUpAt: Date,
    timeoutAt: Date
  ): Promise<void> {
    await this.workflows.updateOne(
      {
        id: workflowId,
      },
      {
        $set: {
          [`naps.${napId}`]: wakeUpAt,
          timeoutAt,
        },
      }
    );
  }
}

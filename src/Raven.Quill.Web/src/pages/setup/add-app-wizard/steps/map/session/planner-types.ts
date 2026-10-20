import type {
    AbsorbedTable,
    CdcSinkTableConfig,
    DroppedTable,
    MigrationFrameProposalFrame,
    ProposedCollection as ProposedCollectionFrame,
} from "@/api/generated/server-api";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import type { SourceTableRef } from "@/pages/setup/add-app-wizard/discover-utils";

/** A table the planner named, with the selected table the name means; null when it fits none. */
type WithTableRef<Named> = Named & { tableRef: SourceTableRef | null };

export type ProposedCollection = Omit<ProposedCollectionFrame, "absorbs"> & {
    rootTableRef: SourceTableRef | null;
    absorbs: WithTableRef<AbsorbedTable>[];
};

export type PlannerProposal = Omit<MigrationFrameProposalFrame, "type" | "collections" | "dropped"> & {
    collections: ProposedCollection[];
    dropped: WithTableRef<DroppedTable>[];
};

/** The selected table a configuration reads from, and the same for each of its embedded tables, in their order. */
export type ConfigTableRef = { table: SourceTableRef | null; embeddedTables: ConfigTableRef[] };

export type SelectedTables = AppFormData["verifySchema"]["tables"];

/** `recommended` indexes into `options`; a question without options is answered in the operator's own words. */
export type PlannerQuestion = { question: string; options: string[]; recommended: number | null };

export type PlannerAnswer = { question: string; answer: string };

export type RegisteredCollection = {
    name: string;
    outcome: "registered";
    version: number;
    /** Unique within the session, unlike the version, which starts over when a removed collection comes back. */
    registrationNumber: number;
    rationale: string | null;
    config: CdcSinkTableConfig | null;
    tableRef: ConfigTableRef | null;
    /** The registration this one replaced, so the details can mark what is new. */
    previousConfig: CdcSinkTableConfig | null;
    warnings: string[];
    /** Why the planner's latest change to it was rejected. The registration itself still stands. */
    rejectionErrors: string[];
};

export type RejectedCollection = { name: string; outcome: "rejected"; errors: string[] };

export type PlannerCollection = RegisteredCollection | RejectedCollection;

export type PlannerActivity =
    | { type: "proposal"; collections: string[]; areaCount: number; droppedCount: number }
    | {
          type: "conventions";
          propertyCase: string;
          propertyLanguage: string | null;
          notes: string | null;
          mustReEmit: string[];
      }
    | { type: "registered"; collection: string; version: number; warningCount: number }
    | { type: "rejected"; collection: string; errorCount: number; isChange: boolean }
    | { type: "removed"; collection: string }
    | { type: "note"; text: string }
    /** The stream failed after the planner had finished the turn, which leaves the turn itself intact. */
    | { type: "lateError"; message: string };

export type PlannerTurnStatus = "working" | "done" | "stopped" | "failed";

export type PlannerPrompt = { text: string; mentions: string[] };

export type PlannerPromptTurn = { kind: "prompt"; id: string } & PlannerPrompt;

export type PlannerAnswersTurn = {
    kind: "answers";
    id: string;
    answers: PlannerAnswer[];
    /** The operator skipped the questions, so every answer is the planner's own recommendation. */
    isSkipped: boolean;
    /** What the operator had written for the planner meanwhile, sent along with the answers. */
    instructions: PlannerPrompt | null;
};

export type PlannerReplyTurn = {
    kind: "planner";
    id: string;
    isOpening: boolean;
    status: PlannerTurnStatus;
    activity: PlannerActivity[];
    reply: string | null;
    gaps: string[];
    /** What the planner asked at the end of the turn. Open until the operator's next turn. */
    questions: PlannerQuestion[];
    error: string | null;
};

export type PlannerTurn = PlannerPromptTurn | PlannerAnswersTurn | PlannerReplyTurn;

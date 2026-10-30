import type {
    AbsorbedTable,
    CdcSinkTableConfig,
    DroppedTable,
    MigrationProposal,
    PlanEntryStatus,
    ProposedArea,
    ProposedCollection as ProposedCollectionDto,
} from "@/api/generated/server-api";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import type { SourceTableRef } from "@/pages/setup/add-app-wizard/discover-utils";

/** A table the planner named, with the selected table the name means; null when it fits none. */
type WithTableRef<Named> = Named & { tableRef: SourceTableRef | null };

export type ProposedCollection = Omit<ProposedCollectionDto, "absorbs"> & {
    rootTableRef: SourceTableRef | null;
    absorbs: WithTableRef<AbsorbedTable>[];
};

export type PlannerArea = Omit<ProposedArea, "collections"> & { collections: ProposedCollection[] };

export type PlannerProposal = Omit<MigrationProposal, "areas" | "dropped"> & {
    areas: PlannerArea[];
    dropped: WithTableRef<DroppedTable>[];
};

/** The selected table a configuration reads from, and the same for each of its embedded tables, in their order. */
export type ConfigTableRef = { table: SourceTableRef | null; embeddedTables: ConfigTableRef[] };

export type SelectedTables = AppFormData["verifySchema"]["tables"];

export type PlannerEntry = {
    name: string;
    area: string | null;
    status: PlanEntryStatus;
    version: number;
    rationale: string | null;
    config: CdcSinkTableConfig | null;
    tableRef: ConfigTableRef | null;
    warnings: string[];
};

export type PlannerEdit = {
    /** The area the collection is in before the save; every change sent for it names this area. */
    area: string;
    /** Null when the mapping is unchanged. A changed collection name in it is a rename. */
    config: CdcSinkTableConfig | null;
    isRemoved: boolean;
};

/** An area's name, or null for the proposal agent's chat. */
export type PlannerChatId = string | null;

export type PlannerView = { area: string | null; collection: string | null };

/** `recommended` indexes into `options`; a question without options is answered in the operator's own words. */
export type PlannerQuestion = { question: string; options: string[]; recommended: number | null };

export type PlannerAnswer = { question: string; answer: string };

export type PlannerActivity =
    | { type: "proposal" }
    | { type: "conventions"; propertyCase: string; propertyLanguage: string | null; notes: string | null }
    | { type: "registered"; collection: string; version: number; warningCount: number }
    | { type: "rejected"; collection: string; errorCount: number; isChange: boolean }
    /** An attempt failed and the agent is still working on the collection. */
    | { type: "transientError"; collection: string; errors: string[] }
    | { type: "removed"; collection: string; reason: string | null }
    | { type: "note"; text: string }
    /** The stream failed after the agent had finished the turn, which leaves the turn itself intact. */
    | { type: "lateError"; message: string };

export type PlannerTurnStatus = "working" | "done" | "stopped" | "failed";

export type PlannerTurnEnding = { error: string | null; isStopped: boolean };

export type PlannerPrompt = { text: string; mentions: string[] };

export type PlannerPromptTurn = { kind: "prompt"; id: string } & PlannerPrompt;

export type PlannerAnswersTurn = {
    kind: "answers";
    id: string;
    answers: PlannerAnswer[];
};

export type PlannerReplyTurn = {
    kind: "planner";
    id: string;
    /** The turn that made the first proposal, or the one that first built an area. */
    isOpening: boolean;
    status: PlannerTurnStatus;
    activity: PlannerActivity[];
    reply: string | null;
    gaps: string[];
    questions: PlannerQuestion[];
    error: string | null;
};

export type PlannerTurn = PlannerPromptTurn | PlannerAnswersTurn | PlannerReplyTurn;

/** What this browser saw of one agent's conversation; the server keeps the whole conversation. */
export type PlannerChat = {
    turns: PlannerTurn[];
    /** Disconnects this chat's stream. Only migrationStop ends the turn on the server. */
    abortController: AbortController | null;
    draft: string;
    mentions: string[];
};

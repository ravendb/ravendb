import { beforeEach, describe, expect, it, vi } from "vitest";
import type { CdcSinkTableConfig, MigrationFrame, MigrationPlanGetResponse } from "@/api/generated/server-api";
import { ApiError } from "@/api/http-client";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { getLatestReply, selectChat } from "@/pages/setup/add-app-wizard/steps/map/session/planner-state";
import type { PlannerChatId } from "@/pages/setup/add-app-wizard/steps/map/session/planner-types";
import { createPlannerSession } from "@/pages/setup/add-app-wizard/steps/map/session/use-planner-session";

const migration = vi.hoisted(() => ({
    proposalStart: vi.fn(),
    proposalAsk: vi.fn(),
    areaStart: vi.fn(),
    areaAsk: vi.fn(),
}));

const setup = vi.hoisted(() => ({ migrationPlanGet: vi.fn(), migrationPlanSave: vi.fn(), migrationStop: vi.fn() }));

vi.mock("@/api/api", () => ({ api: { services: { migration, setup } } }));

vi.mock("sonner", () => ({ toast: { success: vi.fn(), warning: vi.fn(), error: vi.fn() } }));

const DONE: MigrationFrame = { type: "done", conversationId: "c/1", planId: "p/1" };

const EMPTY_PLAN: MigrationPlanGetResponse = { plan: { id: "p/1", entries: {} }, proposal: null };

const CONNECTION = {
    appName: "Shop",
    slug: "shop",
    provider: "SqlClient",
    mode: "raw",
    connectionString: "Server=localhost",
    fields: { host: "", port: null, database: "", username: "", password: "", ssl: "default" },
} satisfies AppFormData["externalConnection"];

const ordersConfig: CdcSinkTableConfig = {
    collectionName: "Orders",
    sourceTableSchema: "dbo",
    sourceTableName: "Orders",
    columns: [{ column: "Id", name: "id", type: "Default" }],
};

async function* streamOf(...frames: MigrationFrame[]): AsyncGenerator<MigrationFrame> {
    yield* frames;
}

/** A stream that stays open until the server ends the turn, as migrationStop does. */
function stoppableStream() {
    let stop: () => void = () => {};
    const stopped = new Promise<void>((resolve) => {
        stop = resolve;
    });

    async function* stream(): AsyncGenerator<MigrationFrame> {
        await stopped;
        yield { type: "error", message: "The turn was stopped." };
    }

    return { stream, stop };
}

function newSession() {
    return createPlannerSession({
        getConnection: () => CONNECTION,
        getSelectedTables: () => [{ sourceTableSchema: "dbo", sourceTableName: "Orders" }],
    });
}

function latestReply(chatId: PlannerChatId) {
    return getLatestReply(selectChat(useSetupWizardStore.getState(), chatId).turns);
}

beforeEach(() => {
    vi.clearAllMocks();
    useSetupWizardStore.getState().reset();
    setup.migrationPlanGet.mockResolvedValue(EMPTY_PLAN);
    setup.migrationStop.mockResolvedValue(undefined);
});

describe("createPlannerSession", () => {
    it("streams the first proposal into the plan chat with the slug only, and takes the plan from its frames", async () => {
        migration.proposalStart.mockImplementation(() => streamOf(DONE));

        await newSession().start();

        expect(migration.proposalStart).toHaveBeenCalledWith(
            { slug: "shop", selectedTables: [{ sourceTableSchema: "dbo", sourceTableName: "Orders" }] },
            expect.any(AbortSignal),
        );
        expect(latestReply(null)?.status).toBe("done");
        expect(setup.migrationPlanGet).not.toHaveBeenCalled();
        expect(useSetupWizardStore.getState().plannerDefaultSchema).toBe("dbo");
    });

    it("reads the plan again after a turn that changes the conventions, since the proposal's property names follow them", async () => {
        migration.proposalAsk.mockImplementation(() =>
            streamOf({ type: "conventions", propertyCase: "CamelCase", propertyLanguage: null, notes: null }, DONE),
        );
        useSetupWizardStore
            .getState()
            .openPlannerSession({ slug: "shop", connectKey: "shop", selectedTables: [], defaultSchema: "dbo" });

        await newSession().askProposal({ text: "Use camelCase.", mentions: [] });

        expect(setup.migrationPlanGet).toHaveBeenCalledWith({ slug: "shop" });
    });

    it("starts an area the first time it builds, and asks its agent for what is missing after that", async () => {
        migration.areaStart.mockImplementation(() => streamOf(DONE));
        migration.areaAsk.mockImplementation(() => streamOf(DONE));
        const session = newSession();
        useSetupWizardStore
            .getState()
            .openPlannerSession({ slug: "shop", connectKey: "shop", selectedTables: [], defaultSchema: "dbo" });

        await session.buildArea("Sales", ["Orders"]);
        await session.buildArea("Sales", ["Products"]);

        expect(migration.areaStart).toHaveBeenCalledOnce();
        expect(migration.areaStart).toHaveBeenCalledWith({ slug: "shop", area: "Sales" }, expect.any(AbortSignal));
        expect(migration.areaAsk).toHaveBeenCalledWith(
            { slug: "shop", area: "Sales", prompt: "Build Products." },
            expect.any(AbortSignal),
        );
    });

    it("starts an area its agent says never started, keeping what the operator wrote for later", async () => {
        migration.areaAsk.mockImplementation(() =>
            streamOf({ type: "error", message: "The area Sales has not started; start it first." }),
        );
        migration.areaStart.mockImplementation(() => streamOf(DONE));
        const store = useSetupWizardStore.getState();
        store.openPlannerSession({ slug: "shop", connectKey: "shop", selectedTables: [], defaultSchema: "dbo" });
        store.setPlannerDraft("Sales", "Embed the lines.");
        store.addPlannerMention("Sales", "Orders");

        await newSession().sendDraft("Sales");

        expect(migration.areaStart).toHaveBeenCalledOnce();
        expect(useSetupWizardStore.getState().plannerAreaChats.Sales).toMatchObject({
            draft: "Embed the lines.",
            mentions: ["Orders"],
        });
        expect(useSetupWizardStore.getState().plannerStartedAreas).toEqual(["Sales"]);
    });

    it("leaves the composer empty when a prompt the operator did not write does not reach the agent", async () => {
        migration.areaAsk.mockImplementation(() =>
            streamOf({ type: "error", message: "The area Sales has not started; start it first." }),
        );
        migration.areaStart.mockImplementation(() => streamOf(DONE));
        useSetupWizardStore
            .getState()
            .openPlannerSession({ slug: "shop", connectKey: "shop", selectedTables: [], defaultSchema: "dbo" });
        useSetupWizardStore.getState().markPlannerAreaStarted("Sales", true);

        await newSession().buildArea("Sales", ["Products"]);

        expect(migration.areaStart).toHaveBeenCalledOnce();
        expect(useSetupWizardStore.getState().plannerAreaChats.Sales.draft).toBe("");
    });

    it("answers an area agent's questions in that area's chat", async () => {
        migration.areaStart.mockImplementation(() =>
            streamOf(
                { type: "reply", reply: "Embed or link?", openQuestions: [{ question: "Lines?", options: ["Embed"] }] },
                DONE,
            ),
        );
        migration.areaAsk.mockImplementation(() => streamOf(DONE));
        useSetupWizardStore
            .getState()
            .openPlannerSession({ slug: "shop", connectKey: "shop", selectedTables: [], defaultSchema: "dbo" });
        const session = newSession();

        await session.buildArea("Sales", ["Orders"]);
        await session.answer("Sales", [{ question: "Lines?", answer: "Embed" }]);

        expect(migration.areaAsk).toHaveBeenCalledWith(
            { slug: "shop", area: "Sales", prompt: "Answers to your open questions:\n1. Lines?\n   Embed" },
            expect.any(AbortSignal),
        );
        expect(migration.proposalAsk).not.toHaveBeenCalled();
    });

    it("stops a turn on the server and lets its stream end by itself", async () => {
        const { stream, stop } = stoppableStream();
        migration.areaStart.mockImplementation(stream);
        setup.migrationStop.mockImplementation(async () => stop());
        useSetupWizardStore
            .getState()
            .openPlannerSession({ slug: "shop", connectKey: "shop", selectedTables: [], defaultSchema: "dbo" });
        const session = newSession();

        const turn = session.buildArea("Sales", ["Orders"]);
        await session.stop("Sales");
        await turn;

        expect(setup.migrationStop).toHaveBeenCalledWith({ slug: "shop", agent: "Area", area: "Sales" });
        expect(latestReply("Sales")?.status).toBe("stopped");
    });

    it("keeps a prompt refused for a running turn in the composer, to send again", async () => {
        migration.proposalAsk.mockImplementation(() => {
            throw new ApiError("a planner turn is already running for this conversation", 409, undefined);
        });
        const store = useSetupWizardStore.getState();
        store.openPlannerSession({ slug: "shop", connectKey: "shop", selectedTables: [], defaultSchema: "dbo" });
        store.setPlannerDraft(null, "Use camelCase.");
        store.addPlannerMention(null, "Orders");

        await newSession().sendDraft(null);

        expect(useSetupWizardStore.getState().plannerProposalChat).toMatchObject({
            draft: "Use camelCase.",
            mentions: ["Orders"],
        });
        expect(latestReply(null)).toMatchObject({
            status: "failed",
            error: "a planner turn is already running for this conversation",
        });
    });

    it("saves every edit in one batch, each change named by kind, and reads the plan again", async () => {
        setup.migrationPlanSave.mockResolvedValue({ issues: [], warnings: ["Loans links to Orders."] });
        const store = useSetupWizardStore.getState();
        store.openPlannerSession({ slug: "shop", connectKey: "shop", selectedTables: [], defaultSchema: "dbo" });
        store.stagePlannerEdit("Orders", {
            area: "Sales",
            config: { ...ordersConfig, collectionName: "Purchases" },
            isRemoved: false,
        });
        store.stagePlannerEdit("AuditLog", { area: "Ops", config: null, isRemoved: true });

        await newSession().save();

        expect(setup.migrationPlanSave).toHaveBeenCalledWith({
            slug: "shop",
            changes: [
                { kind: "Edit", area: "Sales", collection: "Orders", config: ordersConfig },
                { kind: "Rename", area: "Sales", collection: "Orders", newName: "Purchases" },
                { kind: "Delete", area: "Ops", collection: "AuditLog" },
            ],
        });
        expect(useSetupWizardStore.getState()).toMatchObject({
            plannerEdits: {},
            plannerSaveWarnings: ["Loans links to Orders."],
        });
        expect(setup.migrationPlanGet).toHaveBeenCalled();
    });

    it("keeps the edits until the saved plan is read, and follows the open collection to its new name", async () => {
        setup.migrationPlanSave.mockResolvedValue({ issues: [], warnings: [] });
        setup.migrationPlanGet.mockImplementation(async () => {
            expect(Object.keys(useSetupWizardStore.getState().plannerEdits)).toEqual(["Orders"]);
            return EMPTY_PLAN;
        });
        const store = useSetupWizardStore.getState();
        store.openPlannerSession({ slug: "shop", connectKey: "shop", selectedTables: [], defaultSchema: "dbo" });
        store.showPlannerView({ area: "Sales", collection: "Orders" });
        store.stagePlannerEdit("Orders", {
            area: "Sales",
            config: { ...ordersConfig, collectionName: "Purchases" },
            isRemoved: false,
        });

        await newSession().save();

        expect(setup.migrationPlanGet).toHaveBeenCalledOnce();
        expect(useSetupWizardStore.getState()).toMatchObject({
            plannerEdits: {},
            plannerView: { area: "Sales", collection: "Purchases" },
        });
    });

    it("keeps the edits of a refused save, with each issue at its collection", async () => {
        setup.migrationPlanSave.mockRejectedValue(
            new ApiError("Unprocessable", 422, {
                issues: [{ message: "Freight is not a column.", path: "changes[0].columns[0].column" }],
                warnings: [],
            }),
        );
        const store = useSetupWizardStore.getState();
        store.openPlannerSession({ slug: "shop", connectKey: "shop", selectedTables: [], defaultSchema: "dbo" });
        store.stagePlannerEdit("Orders", { area: "Sales", config: ordersConfig, isRemoved: false });

        await newSession().save();

        expect(Object.keys(useSetupWizardStore.getState().plannerEdits)).toEqual(["Orders"]);
        expect(useSetupWizardStore.getState().plannerSaveIssues).toEqual({
            Orders: [{ message: "Freight is not a column.", path: "columns[0].column" }],
        });
    });

    it("stops every running turn on the server when the session is reset", async () => {
        const proposal = stoppableStream();
        const area = stoppableStream();
        migration.proposalAsk.mockImplementation(proposal.stream);
        migration.areaStart.mockImplementation(area.stream);
        const store = useSetupWizardStore.getState();
        store.openPlannerSession({ slug: "shop", connectKey: "shop", selectedTables: [], defaultSchema: "dbo" });
        const session = newSession();

        void session.askProposal({ text: "Add the audit log.", mentions: [] });
        void session.buildArea("Sales", ["Orders"]);
        await vi.waitFor(() => expect(migration.areaStart).toHaveBeenCalled());
        store.resetPlannerState();

        await vi.waitFor(() => expect(setup.migrationStop).toHaveBeenCalledTimes(2));
        expect(setup.migrationStop).toHaveBeenCalledWith({ slug: "shop", agent: "Proposal" });
        expect(setup.migrationStop).toHaveBeenCalledWith({ slug: "shop", agent: "Area", area: "Sales" });
        proposal.stop();
        area.stop();
    });
});

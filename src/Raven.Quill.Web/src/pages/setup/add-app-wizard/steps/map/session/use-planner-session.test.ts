import { beforeEach, describe, expect, it, vi } from "vitest";
import type { MigrationFrame } from "@/api/generated/server-api";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { getLatestReply } from "@/pages/setup/add-app-wizard/steps/map/session/planner-state";
import type { SelectedTables } from "@/pages/setup/add-app-wizard/steps/map/session/planner-types";
import { createPlannerSession } from "@/pages/setup/add-app-wizard/steps/map/session/use-planner-session";

const migration = vi.hoisted(() => ({ start: vi.fn(), ask: vi.fn() }));

vi.mock("@/api/api", () => ({ api: { services: { migration } } }));

const ORDERS: SelectedTables = [{ sourceTableSchema: "dbo", sourceTableName: "Orders" }];

const CUSTOMERS: SelectedTables = [{ sourceTableSchema: "dbo", sourceTableName: "Customers" }];

const DONE: MigrationFrame = { type: "done", conversationId: "planner/1" };

async function* streamOf(...frames: MigrationFrame[]): AsyncGenerator<MigrationFrame> {
    yield* frames;
}

async function* streamUntilAborted(signal: AbortSignal): AsyncGenerator<MigrationFrame> {
    await new Promise<never>((_, reject) => {
        if (signal.aborted) {
            reject(new Error("aborted"));
        }

        signal.addEventListener("abort", () => reject(new Error("aborted")));
    });

    yield* [];
}

function newSession(getSelectedTables: () => SelectedTables = () => ORDERS) {
    return createPlannerSession({ getSlug: () => "shop", getSelectedTables });
}

function latestStatus() {
    return getLatestReply(useSetupWizardStore.getState().plannerTurns)?.status;
}

beforeEach(() => {
    useSetupWizardStore.getState().reset();
    migration.start.mockReset();
    migration.ask.mockReset();
});

describe("createPlannerSession", () => {
    it("sends the instructions written during the opening once the first plan is ready", async () => {
        migration.start.mockImplementation(() => streamOf(DONE));
        migration.ask.mockImplementation(() => streamOf(DONE));
        useSetupWizardStore.getState().setPlannerDraft("Orders are read with their lines.");

        await newSession().start();

        expect(migration.ask).toHaveBeenCalledWith(
            { slug: "shop", conversationId: "planner/1", prompt: "Orders are read with their lines." },
            expect.any(AbortSignal),
        );
        expect(useSetupWizardStore.getState().plannerDraft).toBe("");
    });

    it("keeps the instructions for the next session when the tables changed while the planner opened", async () => {
        let selectedTables = ORDERS;
        migration.start.mockImplementation(() => {
            selectedTables = CUSTOMERS;
            return streamOf(DONE);
        });
        useSetupWizardStore.getState().setPlannerDraft("Orders are read with their lines.");

        await newSession(() => selectedTables).start();

        expect(migration.ask).not.toHaveBeenCalled();
        expect(useSetupWizardStore.getState().plannerDraft).toBe("Orders are read with their lines.");
    });

    it("marks a turn the operator stopped as stopped, not failed", async () => {
        migration.start.mockImplementation((_request: unknown, signal: AbortSignal) => streamUntilAborted(signal));
        const session = newSession();

        const starting = session.start();
        session.stop();
        await starting;

        expect(latestStatus()).toBe("stopped");
    });

    it("drops what a turn still streams once the session was started over", async () => {
        let release = () => {};
        const isReleased = new Promise<void>((resolve) => {
            release = resolve;
        });
        migration.start.mockImplementation(async function* () {
            yield { type: "note", text: "Reading the schema." } satisfies MigrationFrame;
            await isReleased;
            yield {
                type: "collection",
                status: "registered",
                collection: "Orders",
                version: 1,
                warnings: [],
            } satisfies MigrationFrame;
            yield DONE;
        });

        const starting = newSession().start();
        await vi.waitFor(() =>
            expect(getLatestReply(useSetupWizardStore.getState().plannerTurns)?.activity).toHaveLength(1),
        );
        useSetupWizardStore.getState().startPlannerOver();
        release();
        await starting;

        expect(useSetupWizardStore.getState().plannerCollections).toEqual({});
        expect(useSetupWizardStore.getState().plannerTurns).toEqual([]);
    });

    it("sends what the operator wrote meanwhile along with the answers", async () => {
        migration.start.mockImplementation(() =>
            streamOf(
                {
                    type: "reply",
                    reply: "One question first.",
                    gaps: [],
                    openQuestions: [{ question: "Embed lines?", options: ["Yes", "No"], recommended: 0 }],
                },
                DONE,
            ),
        );
        migration.ask.mockImplementation(() => streamOf(DONE));
        const session = newSession();
        await session.start();
        useSetupWizardStore.getState().setPlannerDraft("Keep it small.");

        await session.answer([{ question: "Embed lines?", answer: "Yes" }], false);

        expect(migration.ask).toHaveBeenCalledTimes(1);
        const [{ prompt }] = migration.ask.mock.calls[0] as [{ prompt: string }];
        expect(prompt).toContain("1. Embed lines?\n   Yes");
        expect(prompt).toContain("Keep it small.");
        expect(useSetupWizardStore.getState().plannerDraft).toBe("");
    });

    it("sends collections picked without writing anything as a selection, with no prompt", async () => {
        migration.start.mockImplementation(() => streamOf(DONE));
        migration.ask.mockImplementation(() => streamOf(DONE));
        const session = newSession();
        await session.start();
        useSetupWizardStore.getState().addPlannerMention("Orders");
        useSetupWizardStore.getState().addPlannerMention("Customers");

        await session.sendDraft();

        expect(migration.ask).toHaveBeenCalledWith(
            { slug: "shop", conversationId: "planner/1", selectedCollections: ["Orders", "Customers"] },
            expect.any(AbortSignal),
        );
        expect(useSetupWizardStore.getState().plannerMentions).toEqual([]);
    });

    it("does not send picked collections that are all built already, with nothing written", async () => {
        migration.start.mockImplementation(() =>
            streamOf(
                {
                    type: "collection",
                    status: "registered",
                    collection: "Orders",
                    version: 1,
                    warnings: [],
                },
                DONE,
            ),
        );
        const session = newSession();
        await session.start();
        useSetupWizardStore.getState().addPlannerMention("Orders");

        await session.sendDraft();

        expect(migration.ask).not.toHaveBeenCalled();
        expect(useSetupWizardStore.getState().plannerMentions).toEqual(["Orders"]);
    });

    it("sends picked areas and collections as a selection", async () => {
        migration.start.mockImplementation(() => streamOf(DONE));
        migration.ask.mockImplementation(() => streamOf(DONE));
        const session = newSession();
        await session.start();

        await session.askSelection({ areas: ["Sales"], collections: ["Customers"] });

        expect(migration.ask).toHaveBeenCalledWith(
            { slug: "shop", conversationId: "planner/1", selectedAreas: ["Sales"], selectedCollections: ["Customers"] },
            expect.any(AbortSignal),
        );
    });

    it("sends collections picked meanwhile with the answers as a selection, not as a dangling prompt", async () => {
        migration.start.mockImplementation(() =>
            streamOf(
                {
                    type: "reply",
                    reply: "One question first.",
                    openQuestions: [{ question: "Embed lines?", options: ["Yes", "No"], recommended: 0 }],
                    gaps: [],
                },
                DONE,
            ),
        );
        migration.ask.mockImplementation(() => streamOf(DONE));
        const session = newSession();
        await session.start();
        useSetupWizardStore.getState().addPlannerMention("Orders");

        await session.answer([{ question: "Embed lines?", answer: "Yes" }], false);

        const [request] = migration.ask.mock.calls[0] as [{ prompt: string; selectedCollections?: string[] }];
        expect(request.prompt).not.toContain("About the");
        expect(request.selectedCollections).toEqual(["Orders"]);
        const answersTurn = useSetupWizardStore.getState().plannerTurns.find((turn) => turn.kind === "answers");
        expect(answersTurn).toMatchObject({ instructions: { text: "", mentions: ["Orders"] } });
    });

    it("sends nothing when nothing is written or picked", async () => {
        migration.start.mockImplementation(() => streamOf(DONE));
        const session = newSession();
        await session.start();

        await session.sendDraft();
        await session.askSelection({ areas: [], collections: [] });

        expect(migration.ask).not.toHaveBeenCalled();
    });
});

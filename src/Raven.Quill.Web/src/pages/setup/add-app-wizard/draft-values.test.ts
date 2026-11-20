import { describe, expect, it } from "vitest";
import { plannerDraft, samplePlanView } from "@/mocks/migration-mocks";
import { buildDraftSeed, hasCurrentPlan } from "@/pages/setup/add-app-wizard/draft-values";

const SLUG = "demo-shop";

describe("buildDraftSeed", () => {
    it("continues on the planner step with the plan and every stored value", () => {
        const { values, resumedDraft } = buildDraftSeed(SLUG, plannerDraft, samplePlanView);

        expect(resumedDraft.initialStep).toBe("map");
        expect(values.externalConnection).toMatchObject({
            appName: "Demo shop",
            slug: SLUG,
            provider: "SqlClient",
            mode: "fields",
            fields: { host: "localhost", port: 1433, database: "demo_shop", username: "sa", password: "secret" },
        });
        expect(values.verifySchema.tables).toEqual(plannerDraft.selectedTables);
        expect(resumedDraft.storeState.discoverResult).toBe(plannerDraft.discovery);
        expect(resumedDraft.storeState.connectKey).not.toBeNull();
        expect(resumedDraft.storeState.planner?.plan).toBe(samplePlanView);
        expect(resumedDraft.storeState.planner?.session).toMatchObject({ slug: SLUG, defaultSchema: "dbo" });
    });

    it("continues on the verify step when the wizard had not moved past it", () => {
        const draft = { ...plannerDraft, step: "verifySchema", hasPlan: false };

        expect(buildDraftSeed(SLUG, draft, null).resumedDraft.initialStep).toBe("verifySchema");
    });

    it("starts at the connect step when nothing was discovered, keeping the connection", () => {
        const { values, resumedDraft } = buildDraftSeed(SLUG, { ...plannerDraft, discovery: null }, null);

        expect(resumedDraft.initialStep).toBe("externalConnection");
        expect(values.externalConnection.fields.database).toBe("demo_shop");
        expect(values.verifySchema.tables).toEqual([]);
        expect(resumedDraft.storeState.connectKey).toBeNull();
    });

    it("lets the slug follow the app name only while it still derives from it", () => {
        expect(buildDraftSeed("demo-shop", plannerDraft, null).resumedDraft.storeState.hasEditedSlug).toBe(false);
        expect(buildDraftSeed("my-shop", plannerDraft, null).resumedDraft.storeState.hasEditedSlug).toBe(true);
    });
});

describe("hasCurrentPlan", () => {
    it("drops a plan made from another selection, which starting the planner over left behind", () => {
        const draft = { ...plannerDraft, selectedTables: plannerDraft.selectedTables.slice(1) };

        expect(hasCurrentPlan(plannerDraft)).toBe(true);
        expect(hasCurrentPlan(draft)).toBe(false);
        expect(buildDraftSeed(SLUG, draft, samplePlanView).resumedDraft.storeState.planner).toBeNull();
    });
});

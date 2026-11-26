import { describe, expect, it } from "vitest";
import { sampleAppCdcConfiguration } from "@/mocks/apps-mocks";
import { plannerDraft, samplePlanView } from "@/mocks/migration-mocks";
import { sampleDashboardApps } from "@/mocks/stats-mocks";
import { buildEditAppSeed, isPlanAsApplied } from "@/pages/setup/add-app-wizard/edit-app-values";

const APP = sampleDashboardApps[0];

describe("isPlanAsApplied", () => {
    it("holds while every registered collection is at the version that was applied", () => {
        expect(isPlanAsApplied(plannerDraft, samplePlanView)).toBe(true);
    });

    it("fails once a collection was edited, added or removed since", () => {
        expect(
            isPlanAsApplied(
                { ...plannerDraft, appliedPlanVersions: { Customers: 1, Orders: 1, Products: 1 } },
                samplePlanView,
            ),
        ).toBe(false);
        expect(
            isPlanAsApplied({ ...plannerDraft, appliedPlanVersions: { Customers: 1, Orders: 2 } }, samplePlanView),
        ).toBe(false);
    });

    it("fails for a mapping made by hand", () => {
        expect(isPlanAsApplied({ ...plannerDraft, appliedPlanVersions: null }, samplePlanView)).toBe(false);
    });
});

describe("buildEditAppSeed", () => {
    it("reopens the applied plan with the tables it was handed", () => {
        const seed = buildEditAppSeed(APP, sampleAppCdcConfiguration, plannerDraft, samplePlanView);

        if ("error" in seed) {
            throw new Error(seed.error);
        }

        expect(seed.editedApp.planner?.plan).toBe(samplePlanView);
        expect(seed.values.verifySchema.tables).toEqual(plannerDraft.planSelectedTables);
        expect(seed.editedApp.initialSelectedTables).toEqual(plannerDraft.planSelectedTables);
    });

    it("leaves an app without an applied plan to the editor", () => {
        const seed = buildEditAppSeed(APP, sampleAppCdcConfiguration, null, null);

        if ("error" in seed) {
            throw new Error(seed.error);
        }

        expect(seed.editedApp.planner).toBeNull();
        expect(seed.values.mapTables.tables.length).toBeGreaterThan(0);
    });
});

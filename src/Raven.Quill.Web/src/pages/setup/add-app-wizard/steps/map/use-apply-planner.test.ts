import { describe, expect, it } from "vitest";
import type { PlannerEntry } from "@/pages/setup/add-app-wizard/steps/map/session/planner-types";
import { computePlanKey, isPlanApplied } from "@/pages/setup/add-app-wizard/steps/map/use-apply-planner";

const entry = (name: string, version: number, status: PlannerEntry["status"] = "Registered"): PlannerEntry => ({
    name,
    area: "Sales",
    status,
    version,
    rationale: null,
    config: null,
    tableRef: null,
    warnings: [],
});

const entries = (...list: PlannerEntry[]) => Object.fromEntries(list.map((item) => [item.name, item]));

describe("computePlanKey", () => {
    it("does not depend on the order of the entries", () => {
        expect(computePlanKey("shop", entries(entry("Orders", 1), entry("Products", 2)))).toBe(
            computePlanKey("shop", entries(entry("Products", 2), entry("Orders", 1))),
        );
    });

    it("changes with every new version of a collection", () => {
        expect(computePlanKey("shop", entries(entry("Orders", 1)))).not.toBe(
            computePlanKey("shop", entries(entry("Orders", 2))),
        );
    });

    it("leaves out what apply leaves out: rejected and removed collections", () => {
        expect(
            computePlanKey(
                "shop",
                entries(entry("Orders", 1), entry("Products", 1, "Rejected"), entry("Audit", 2, "Removed")),
            ),
        ).toBe(computePlanKey("shop", entries(entry("Orders", 1))));
    });

    it("changes with the app", () => {
        expect(computePlanKey("shop", entries(entry("Orders", 1)))).not.toBe(
            computePlanKey("store", entries(entry("Orders", 1))),
        );
    });
});

describe("isPlanApplied", () => {
    const planKey = computePlanKey("shop", entries(entry("Orders", 1)));

    it("matches the plan the mapping was applied from", () => {
        expect(isPlanApplied({ source: "planner", planKey }, planKey)).toBe(true);
    });

    it("never matches a mapping made by hand or imported, so applying a plan over one is never skipped", () => {
        expect(isPlanApplied({ source: "manual" }, planKey)).toBe(false);
        expect(isPlanApplied({ source: "import" }, planKey)).toBe(false);
    });
});

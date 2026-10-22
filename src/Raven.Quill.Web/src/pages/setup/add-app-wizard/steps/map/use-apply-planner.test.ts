import { describe, expect, it } from "vitest";
import type { RegisteredCollection } from "@/pages/setup/add-app-wizard/steps/map/session/planner-types";
import { computePlanKey, isPlanApplied } from "@/pages/setup/add-app-wizard/steps/map/use-apply-planner";

const collection = (name: string, registrationNumber: number): RegisteredCollection => ({
    name,
    outcome: "registered",
    version: 1,
    registrationNumber,
    rationale: null,
    config: null,
    tableRef: null,
    previousConfig: null,
    warnings: [],
    rejectionErrors: [],
});

describe("computePlanKey", () => {
    it("does not depend on the order the collections arrived in", () => {
        expect(computePlanKey("c/1", [collection("Orders", 1), collection("Products", 2)])).toBe(
            computePlanKey("c/1", [collection("Products", 2), collection("Orders", 1)]),
        );
    });

    it("changes with every registration, also one whose version started over after a removal", () => {
        expect(computePlanKey("c/1", [collection("Orders", 1)])).not.toBe(
            computePlanKey("c/1", [collection("Orders", 2)]),
        );
    });

    it("changes when the selection changes", () => {
        expect(computePlanKey("c/1", [collection("Orders", 1)])).not.toBe(
            computePlanKey("c/1", [collection("Orders", 1), collection("Products", 1)]),
        );
    });

    it("changes with the session", () => {
        expect(computePlanKey("c/1", [collection("Orders", 1)])).not.toBe(
            computePlanKey("c/2", [collection("Orders", 1)]),
        );
    });
});

describe("isPlanApplied", () => {
    const planKey = computePlanKey("c/1", [collection("Orders", 1)]);

    it("matches the plan the mapping was applied from", () => {
        expect(isPlanApplied({ source: "planner", planKey }, planKey)).toBe(true);
    });

    it("never matches a mapping made by hand or imported, so applying a plan over one is never skipped", () => {
        expect(isPlanApplied({ source: "manual" }, planKey)).toBe(false);
        expect(isPlanApplied({ source: "import" }, planKey)).toBe(false);
    });
});

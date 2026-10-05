import { describe, expect, it } from "vitest";
import type { PlannerCollection } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { computePlannerAppliedKey } from "@/pages/setup/add-app-wizard/steps/map/use-apply-planner";

const collection = (name: string, version: number): PlannerCollection => ({
    collection: name,
    version,
    status: "registered",
    warnings: [],
});

describe("computePlannerAppliedKey", () => {
    it("does not depend on the order the collections arrived in", () => {
        expect(computePlannerAppliedKey("c/1", [collection("Orders", 1), collection("Products", 2)])).toBe(
            computePlannerAppliedKey("c/1", [collection("Products", 2), collection("Orders", 1)]),
        );
    });

    it("changes when a collection is re-emitted", () => {
        expect(computePlannerAppliedKey("c/1", [collection("Orders", 1)])).not.toBe(
            computePlannerAppliedKey("c/1", [collection("Orders", 2)]),
        );
    });

    it("changes when the selection changes", () => {
        expect(computePlannerAppliedKey("c/1", [collection("Orders", 1)])).not.toBe(
            computePlannerAppliedKey("c/1", [collection("Orders", 1), collection("Products", 1)]),
        );
    });

    it("changes with the session", () => {
        expect(computePlannerAppliedKey("c/1", [collection("Orders", 1)])).not.toBe(
            computePlannerAppliedKey("c/2", [collection("Orders", 1)]),
        );
    });
});

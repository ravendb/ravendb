import { describe, expect, it } from "vitest";
import type { RegisteredCollection } from "@/pages/setup/add-app-wizard/steps/map/planner-types";
import { computePlannerOriginKey } from "@/pages/setup/add-app-wizard/steps/map/use-apply-planner";
import { IMPORTED_MAP_ORIGIN, MANUAL_MAP_ORIGIN } from "@/pages/setup/add-app-wizard/steps/map/use-adopt-map-tables";

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

describe("computePlannerOriginKey", () => {
    it("does not depend on the order the collections arrived in", () => {
        expect(computePlannerOriginKey("c/1", [collection("Orders", 1), collection("Products", 2)])).toBe(
            computePlannerOriginKey("c/1", [collection("Products", 2), collection("Orders", 1)]),
        );
    });

    it("changes with every registration, also one whose version started over after a removal", () => {
        expect(computePlannerOriginKey("c/1", [collection("Orders", 1)])).not.toBe(
            computePlannerOriginKey("c/1", [collection("Orders", 2)]),
        );
    });

    it("changes when the selection changes", () => {
        expect(computePlannerOriginKey("c/1", [collection("Orders", 1)])).not.toBe(
            computePlannerOriginKey("c/1", [collection("Orders", 1), collection("Products", 1)]),
        );
    });

    it("changes with the session", () => {
        expect(computePlannerOriginKey("c/1", [collection("Orders", 1)])).not.toBe(
            computePlannerOriginKey("c/2", [collection("Orders", 1)]),
        );
    });

    it("never matches a mapping made by hand or imported, so applying a plan over one is never skipped", () => {
        const key = computePlannerOriginKey("c/1", [collection("Orders", 1)]);

        expect([MANUAL_MAP_ORIGIN, IMPORTED_MAP_ORIGIN]).not.toContain(key);
    });
});

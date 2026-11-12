import { describe, expect, it } from "vitest";
import { getAppFlow } from "@/pages/setup/add-app-wizard/app-wizard-flow";

describe("getAppFlow", () => {
    it("ends on the planner step until a mapping exists", () => {
        expect(getAppFlow({ dataSource: "external", mapOrigin: null })).toEqual([
            "externalConnection",
            "verifySchema",
            "map",
        ]);
    });

    it("keeps the planner step in front of the editor for a manual mapping", () => {
        expect(getAppFlow({ dataSource: "external", mapOrigin: { source: "manual" } })).toEqual([
            "externalConnection",
            "verifySchema",
            "map",
            "mapTables",
        ]);
    });

    it("skips the planner step for an imported mapping", () => {
        expect(getAppFlow({ dataSource: "external", mapOrigin: { source: "import" } })).toEqual([
            "externalConnection",
            "verifySchema",
            "mapTables",
        ]);
    });
});

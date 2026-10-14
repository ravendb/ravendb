import { describe, expect, it } from "vitest";
import {
    composeAddTablesPrompt,
    composeBuildCollectionsPrompt,
    composePlannerPrompt,
} from "@/pages/setup/add-app-wizard/steps/map/planner-prompts";

describe("composePlannerPrompt", () => {
    it("sends a prompt without mentions as typed", () => {
        expect(composePlannerPrompt({ text: "Embed the lines.", mentions: [] })).toBe("Embed the lines.");
    });

    it("leads with the mentioned collections, so the planner knows what the words are about", () => {
        expect(composePlannerPrompt({ text: "Link them by id.", mentions: ["Orders", "Products"] })).toBe(
            "About the Orders and Products collections: Link them by id.",
        );
    });
});

describe("composeAddTablesPrompt", () => {
    it("asks for one table or a list of them", () => {
        expect(composeAddTablesPrompt(["dbo.AuditLog"])).toBe("Add dbo.AuditLog to the plan.");
        expect(composeAddTablesPrompt(["log.Errors", "log.Jobs", "log.Logins"])).toBe(
            "Add these tables to the plan: log.Errors, log.Jobs and log.Logins.",
        );
    });
});

describe("composeBuildCollectionsPrompt", () => {
    it("names every collection, so the planner builds exactly those", () => {
        expect(composeBuildCollectionsPrompt(["Orders", "Products", "Customers"])).toBe(
            "Build Orders, Products and Customers.",
        );
    });
});

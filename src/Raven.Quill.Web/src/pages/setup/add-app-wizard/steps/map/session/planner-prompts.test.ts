import { describe, expect, it } from "vitest";
import {
    composeAddTablesPrompt,
    composeAnswersPrompt,
    composeBuildCollectionsPrompt,
    composeFixCollectionPrompt,
    composePlannerPrompt,
} from "@/pages/setup/add-app-wizard/steps/map/session/planner-prompts";

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

describe("composeAnswersPrompt", () => {
    const answers = [
        { question: "Embed order lines?", answer: "Link" },
        { question: "Copy the unit price onto lines?", answer: "Ignore" },
    ];

    it("repeats each question with its answer, since the planner reads the answers as a fresh message", () => {
        expect(composeAnswersPrompt({ answers })).toBe(
            "Answers to your open questions:\n" +
                "1. Embed order lines?\n   Link\n" +
                "2. Copy the unit price onto lines?\n   Ignore",
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

describe("composeFixCollectionPrompt", () => {
    it("tells the agent why the collection was rejected", () => {
        expect(composeFixCollectionPrompt("Products", ["Price is mapped twice."])).toBe(
            "Fix the Products collection. It was rejected: Price is mapped twice.",
        );
    });
});

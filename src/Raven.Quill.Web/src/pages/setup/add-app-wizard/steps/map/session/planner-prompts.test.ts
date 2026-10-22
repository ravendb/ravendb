import { describe, expect, it } from "vitest";
import {
    composeAddTablesPrompt,
    composeAnswersPrompt,
    composeBuildCollectionsPrompt,
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
        expect(composeAnswersPrompt({ answers, isSkipped: false, instructions: null })).toBe(
            "Answers to your open questions:\n" +
                "1. Embed order lines?\n   Link\n" +
                "2. Copy the unit price onto lines?\n   Ignore",
        );
    });

    it("tells the planner the questions were skipped, so it can tell a skip from real answers", () => {
        expect(composeAnswersPrompt({ answers, isSkipped: true, instructions: null })).toMatch(
            /^I skipped your open questions - go with your recommendations:\n/,
        );
    });

    it("adds what the operator wrote for the planner meanwhile", () => {
        expect(
            composeAnswersPrompt({
                answers: answers.slice(0, 1),
                isSkipped: false,
                instructions: { text: "Orders are always read with their lines.", mentions: ["Orders"] },
            }),
        ).toBe(
            "Answers to your open questions:\n" +
                "1. Embed order lines?\n   Link\n\n" +
                "About the Orders collection: Orders are always read with their lines.",
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

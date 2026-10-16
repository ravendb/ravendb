import { describe, expect, it } from "vitest";
import {
    answerSchema,
    CUSTOM_ANSWER,
    getInitialAnswers,
    recommendedAnswers,
    resolveAnswers,
} from "@/pages/setup/add-app-wizard/steps/map/chat/planner-questions-utils";
import type { PlannerQuestion } from "@/pages/setup/add-app-wizard/steps/map/session/planner-types";

const embedLines: PlannerQuestion = {
    question: "Embed order lines?",
    options: ["Embed", "Link", "Drop"],
    recommended: 0,
};
const pricing: PlannerQuestion = {
    question: "Copy the unit price onto lines?",
    options: ["Copy it", "Reference the product", "Ignore"],
    recommended: null,
};
const naming: PlannerQuestion = { question: "What should the app call a customer?", options: [], recommended: null };

describe("getInitialAnswers", () => {
    it("preselects the recommendation, leaves a question without one open, and takes words where there are no options", () => {
        expect(getInitialAnswers([embedLines, pricing, naming]).answers).toEqual([
            { choice: "0", custom: "" },
            { choice: "", custom: "" },
            { choice: CUSTOM_ANSWER, custom: "" },
        ]);
    });
});

describe("answerSchema", () => {
    it("takes a picked option or words, but not an empty answer", () => {
        expect(answerSchema.safeParse({ choice: "2", custom: "" }).success).toBe(true);
        expect(answerSchema.safeParse({ choice: CUSTOM_ANSWER, custom: "Client" }).success).toBe(true);
        expect(answerSchema.safeParse({ choice: CUSTOM_ANSWER, custom: "  " }).success).toBe(false);
        expect(answerSchema.safeParse({ choice: "", custom: "" }).success).toBe(false);
    });
});

describe("resolveAnswers", () => {
    it("pairs each question with the picked option or the operator's words", () => {
        expect(
            resolveAnswers(
                [embedLines, naming],
                [
                    { choice: "1", custom: "" },
                    { choice: CUSTOM_ANSWER, custom: " Client " },
                ],
            ),
        ).toEqual([
            { question: "Embed order lines?", answer: "Link" },
            { question: "What should the app call a customer?", answer: "Client" },
        ]);
    });
});

describe("recommendedAnswers", () => {
    it("answers every question with the planner's recommendation, whatever was picked", () => {
        expect(recommendedAnswers([embedLines, pricing])).toEqual([
            { question: "Embed order lines?", answer: "Embed" },
            { question: "Copy the unit price onto lines?", answer: "No preference - use your judgement." },
        ]);
    });
});

import { describe, expect, it } from "vitest";
import {
    areAllQuestionsAnswered,
    composeAnswersPrompt,
    recommendedAnswers,
    splitOpenQuestions,
    type PlannerQuestion,
} from "@/pages/setup/add-app-wizard/steps/map/planner-questions-utils";

const embedLines: PlannerQuestion = {
    question: "Embed order lines?",
    options: ["Embed", "Link", "Drop"],
    recommended: 0,
};
const pricing: PlannerQuestion = {
    question: "Copy the unit price onto lines?",
    options: ["Copy it", "Reference the product", "Ignore"],
    recommended: 1,
};

describe("splitOpenQuestions", () => {
    it("sends questions with answers to the picker and the rest to the transcript", () => {
        const result = splitOpenQuestions([
            { question: "Embed order lines?", options: ["Embed", "Link", "Drop"], recommended: 0 },
            { question: "Anything else?", options: [], recommended: null },
        ]);

        expect(result.pickable).toEqual([embedLines]);
        expect(result.freeForm).toEqual(["Anything else?"]);
    });

    it("treats a recommendation outside the answers as unanswerable by skipping", () => {
        const result = splitOpenQuestions([{ question: "q", options: ["a", "b"], recommended: 5 }]);

        expect(result.pickable).toEqual([]);
        expect(result.freeForm).toEqual(["q"]);
    });
});

describe("areAllQuestionsAnswered", () => {
    it("holds until every question has a pick", () => {
        expect(areAllQuestionsAnswered([embedLines, pricing], [0, undefined])).toBe(false);
        expect(areAllQuestionsAnswered([embedLines, pricing], [2, 0])).toBe(true);
    });
});

describe("composeAnswersPrompt", () => {
    it("pairs each picked answer with its question", () => {
        expect(composeAnswersPrompt([embedLines, pricing], [1, 2], false)).toBe(
            "Answers to your open questions:\n" +
                "1. Embed order lines?\n   Link\n" +
                "2. Copy the unit price onto lines?\n   Ignore",
        );
    });

    it("applies the recommendations when skipped", () => {
        const prompt = composeAnswersPrompt([embedLines, pricing], recommendedAnswers([embedLines, pricing]), true);

        expect(prompt).toBe(
            "I skipped your open questions - go with your recommendations:\n" +
                "1. Embed order lines?\n   Embed\n" +
                "2. Copy the unit price onto lines?\n   Reference the product",
        );
    });
});

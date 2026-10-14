import { z } from "zod";
import { composePlannerPrompt } from "@/pages/setup/add-app-wizard/steps/map/planner-prompts";
import type {
    PlannerAnswer,
    PlannerAnswersTurn,
    PlannerQuestion,
} from "@/pages/setup/add-app-wizard/steps/map/planner-types";

/** The choice of an answer in the operator's own words rather than one of the options. */
export const CUSTOM_ANSWER = "custom";

const NO_PREFERENCE = "No preference - use your judgement.";

/** `choice` is the picked option's index, `CUSTOM_ANSWER`, or empty while nothing is picked. */
export const answerSchema = z
    .object({ choice: z.string(), custom: z.string() })
    .refine((answer) => answer.choice !== "" && (answer.choice !== CUSTOM_ANSWER || answer.custom.trim() !== ""), {
        message: "Pick an answer, or write your own.",
        path: ["custom"],
    });

export const answersFormSchema = z.object({ answers: z.array(answerSchema) });

export type AnswersFormData = z.infer<typeof answersFormSchema>;

/** The recommended option comes preselected, so accepting every recommendation takes no clicks. */
export function getInitialAnswers(questions: PlannerQuestion[]): AnswersFormData {
    return {
        answers: questions.map((question) => ({ choice: getInitialChoice(question), custom: "" })),
    };
}

export function resolveAnswers(questions: PlannerQuestion[], answers: AnswersFormData["answers"]): PlannerAnswer[] {
    return questions.map((question, index) => {
        const { choice, custom } = answers[index];

        return {
            question: question.question,
            answer: choice === CUSTOM_ANSWER ? custom.trim() : question.options[Number(choice)],
        };
    });
}

export function recommendedAnswers(questions: PlannerQuestion[]): PlannerAnswer[] {
    return questions.map((question) => ({
        question: question.question,
        answer: question.recommended === null ? NO_PREFERENCE : question.options[question.recommended],
    }));
}

/**
 * The answers as the next turn. The question goes along with each answer because the planner reads this
 * as a fresh message, and "Link" on its own does not say what it answers.
 */
export function composeAnswersPrompt({
    answers,
    isSkipped,
    instructions,
}: Pick<PlannerAnswersTurn, "answers" | "isSkipped" | "instructions">): string {
    const heading = isSkipped
        ? "I skipped your open questions - go with your recommendations:"
        : "Answers to your open questions:";
    const lines = answers.map(({ question, answer }, index) => `${index + 1}. ${question}\n   ${answer}`);
    const answersPrompt = [heading, ...lines].join("\n");

    return instructions ? `${answersPrompt}\n\n${composePlannerPrompt(instructions)}` : answersPrompt;
}

function getInitialChoice(question: PlannerQuestion): string {
    if (question.options.length === 0) {
        return CUSTOM_ANSWER;
    }

    return question.recommended === null ? "" : String(question.recommended);
}

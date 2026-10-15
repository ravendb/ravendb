import { z } from "zod";
import type { PlannerAnswer, PlannerQuestion } from "@/pages/setup/add-app-wizard/steps/map/session/planner-types";

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

function getInitialChoice(question: PlannerQuestion): string {
    if (question.options.length === 0) {
        return CUSTOM_ANSWER;
    }

    return question.recommended === null ? "" : String(question.recommended);
}

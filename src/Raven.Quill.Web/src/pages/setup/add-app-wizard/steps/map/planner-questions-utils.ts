import type { MigrationFrame } from "@/api/custom-services/migration-service";

type OpenQuestionFrame = Extract<MigrationFrame, { type: "reply" }>["openQuestions"][number];

/** A question the operator answers by picking. `recommended` indexes into `options`. */
export type PlannerQuestion = { question: string; options: string[]; recommended: number };

/** Questions offered with answers go to the picker; the rest stay in the transcript as prose. */
export function splitOpenQuestions(openQuestions: OpenQuestionFrame[]): {
    pickable: PlannerQuestion[];
    freeForm: string[];
} {
    const pickable: PlannerQuestion[] = [];
    const freeForm: string[] = [];

    for (const { question, options, recommended } of openQuestions) {
        if (options.length > 0 && recommended != null && recommended >= 0 && recommended < options.length) {
            pickable.push({ question, options, recommended });
        } else {
            freeForm.push(question);
        }
    }

    return { pickable, freeForm };
}

/** The recommended answer for every question - what a skip applies. */
export function recommendedAnswers(questions: PlannerQuestion[]): number[] {
    return questions.map((question) => question.recommended);
}

export function areAllQuestionsAnswered(questions: PlannerQuestion[], answers: (number | undefined)[]): boolean {
    return questions.every((_, index) => answers[index] !== undefined);
}

/**
 * The operator's picks as the next turn. The question goes along with each answer because the
 * agent reads this as a fresh message, and "Link" on its own does not say what it answers.
 */
export function composeAnswersPrompt(
    questions: PlannerQuestion[],
    answers: (number | undefined)[],
    isSkipped: boolean,
): string {
    const lines = questions.map((question, index) => {
        const answer = question.options[answers[index] ?? question.recommended];
        return `${index + 1}. ${question.question}\n   ${answer}`;
    });

    const heading = isSkipped
        ? "I skipped your open questions - go with your recommendations:"
        : "Answers to your open questions:";

    return [heading, ...lines].join("\n");
}

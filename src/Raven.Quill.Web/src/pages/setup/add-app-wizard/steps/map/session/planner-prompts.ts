import { formatCount, joinList } from "@/lib/format";
import type { PlannerAnswersTurn, PlannerPrompt } from "@/pages/setup/add-app-wizard/steps/map/session/planner-types";

/** The prompt as the planner receives it: mentioned collections lead, so it knows what the words are about. */
export function composePlannerPrompt({ text, mentions }: PlannerPrompt): string {
    if (mentions.length === 0) {
        return text;
    }

    const subject = mentions.length === 1 ? "collection" : "collections";

    return `About the ${joinList(mentions)} ${subject}: ${text}`;
}

/**
 * The answers as the next turn. The question goes along with each answer because the planner reads this
 * as a fresh message, and "Link" on its own does not say what it answers.
 */
export function composeAnswersPrompt({ answers }: Pick<PlannerAnswersTurn, "answers">): string {
    const lines = answers.map(({ question, answer }, index) => `${index + 1}. ${question}\n   ${answer}`);

    return ["Answers to your open questions:", ...lines].join("\n");
}

export function composeAddTablesPrompt(tables: string[]): string {
    return tables.length === 1 ? `Add ${tables[0]} to the plan.` : `Add these tables to the plan: ${joinList(tables)}.`;
}

export function composeBuildCollectionsPrompt(collections: string[]): string {
    return `Build ${joinList(collections)}.`;
}

export function composeFixCollectionPrompt(collection: string, problems: string[]): string {
    return problems.length > 0
        ? `Fix the ${collection} collection. It was rejected: ${problems.join(" ")}`
        : `Fix the ${collection} collection.`;
}

export function composeConventionsPrompt(propertyNames: string): string {
    return `Use ${propertyNames} for property names in every collection.`;
}

export function describeMentionPlaceholder(mentions: string[]): string {
    return mentions.length === 1
        ? `Ask about ${mentions[0]}, or tell the planner what to change in it.`
        : `Ask about these ${formatCount(mentions.length, "collection")}, or tell the planner what to change in them.`;
}

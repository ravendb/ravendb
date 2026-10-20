import { formatCount, joinList } from "@/lib/format";
import { UNSPECIFIED_PROPERTY_CASE } from "@/pages/setup/add-app-wizard/steps/map/session/planner-frames";
import {
    getRegisteredState,
    isBuilt,
    type PlanCollection,
} from "@/pages/setup/add-app-wizard/steps/map/session/planner-plan";
import { getLatestReply, type PlannerState } from "@/pages/setup/add-app-wizard/steps/map/session/planner-state";
import type { PlannerActivity, PlannerReplyTurn } from "@/pages/setup/add-app-wizard/steps/map/session/planner-types";

/** `kept` are built collections whose latest change was rejected: they stay as they were built. */
export type TurnOutcomes = { built: string[]; updated: string[]; failed: string[]; kept: string[]; removed: string[] };

export type PlanProgress = { builtCount: number; totalCount: number };

export type ActivityHeadline = { title: string; detail: string; isDetailDestructive: boolean };

/** `keeps` names the built collections whose rejected change the reply lets go. */
export type SuggestedReply = { label: string; prompt: string; isRetry: boolean; keeps: string[] };

const PROPERTY_CASE_LABELS: Record<string, string> = {
    CamelCase: "camelCase",
    PascalCase: "PascalCase",
    SnakeCase: "snake_case",
};

/** Where each collection the turn touched ended up, its last frame deciding. */
export function getTurnOutcomes(activity: PlannerActivity[]): TurnOutcomes {
    const outcomes = new Map<string, keyof TurnOutcomes>();

    for (const item of activity) {
        switch (item.type) {
            case "registered":
                outcomes.set(item.collection, getRegisteredState(item.version));
                break;
            case "rejected":
                outcomes.set(item.collection, item.isChange ? "kept" : "failed");
                break;
            case "removed":
                outcomes.set(item.collection, "removed");
                break;
        }
    }

    const namesWith = (outcome: keyof TurnOutcomes) =>
        [...outcomes].filter(([, value]) => value === outcome).map(([name]) => name);

    return {
        built: namesWith("built"),
        updated: namesWith("updated"),
        failed: namesWith("failed"),
        kept: namesWith("kept"),
        removed: namesWith("removed"),
    };
}

export function hasCollectionOutcomes(outcomes: TurnOutcomes): boolean {
    return Object.values(outcomes).some((names) => names.length > 0);
}

export function isBuildingCollections(turn: PlannerReplyTurn): boolean {
    return turn.activity.some(
        (activity) => activity.type === "registered" || activity.type === "rejected" || activity.type === "transientError",
    );
}

export function selectIsBuildingCollections(state: PlannerState): boolean {
    const latestReply = getLatestReply(state.plannerTurns);

    return latestReply?.status === "working" && isBuildingCollections(latestReply);
}

/** The one line a turn's tool activity folds into, e.g. "Built 2 of 3 collections · 1 failed". */
export function getActivityHeadline(turn: PlannerReplyTurn, progress: PlanProgress): ActivityHeadline {
    const builtProgress = `${progress.builtCount} of ${progress.totalCount} built`;

    if (turn.status === "working") {
        return {
            title: describeWork(turn),
            detail: isBuildingCollections(turn) ? builtProgress : "",
            isDetailDestructive: false,
        };
    }

    const outcomes = getTurnOutcomes(turn.activity);

    if (outcomes.failed.length > 0) {
        return { title: getDoneTitle(outcomes), detail: `${outcomes.failed.length} failed`, isDetailDestructive: true };
    }

    if (outcomes.kept.length > 0) {
        return {
            title: getDoneTitle(outcomes),
            detail: `${formatCount(outcomes.kept.length, "change")} rejected`,
            isDetailDestructive: true,
        };
    }

    return { title: getDoneTitle(outcomes), detail: builtProgress, isDetailDestructive: false };
}

export function describeConventions(activity: Extract<PlannerActivity, { type: "conventions" }>): string {
    const parts = [
        activity.propertyCase === UNSPECIFIED_PROPERTY_CASE
            ? null
            : `Property names set to ${PROPERTY_CASE_LABELS[activity.propertyCase] ?? activity.propertyCase}`,
        activity.propertyLanguage ? `Property names in ${activity.propertyLanguage}` : null,
        activity.mustReEmit.length > 0 ? `re-emitting ${joinList(activity.mustReEmit)}` : null,
    ].filter((part) => part !== null);

    return parts.length > 0 ? parts.join(", ") : "Naming conventions updated";
}

export function describeWork(turn: PlannerReplyTurn): string {
    return isBuildingCollections(turn) ? "Building collections" : "Working on your prompt";
}

/** Before anything is built, the whole proposal is one click away instead of a prompt to write. */
export function getCollectionsToBuildFirst(collections: PlanCollection[]): string[] {
    if (collections.some(isBuilt)) {
        return [];
    }

    return collections.filter((collection) => collection.state === "suggested").map((collection) => collection.name);
}

export function getSuggestedReplies({ failed, kept }: Pick<TurnOutcomes, "failed" | "kept">): SuggestedReply[] {
    const replies: SuggestedReply[] = [];

    if (failed.length > 0) {
        const names = joinList(failed);

        replies.push(
            {
                label: failed.length === 1 ? `Retry ${names}` : `Retry the ${failed.length} failed`,
                prompt: `Retry ${names}.`,
                isRetry: true,
                keeps: [],
            },
            {
                label: failed.length === 1 ? `Carry on without ${names}` : "Carry on without them",
                prompt: `Carry on without ${names}.`,
                isRetry: false,
                keeps: [],
            },
        );
    }

    if (kept.length > 0) {
        const names = joinList(kept);

        replies.push(
            {
                label: kept.length === 1 ? `Retry the change to ${names}` : `Retry the ${kept.length} changes`,
                prompt: `Retry the change to ${names}.`,
                isRetry: true,
                keeps: [],
            },
            {
                label: kept.length === 1 ? "Keep the previous version" : "Keep the previous versions",
                prompt: `Keep ${names} as ${kept.length === 1 ? "it is" : "they are"}.`,
                isRetry: false,
                keeps: kept,
            },
        );
    }

    return replies;
}

function getDoneTitle({ built, updated, failed, kept, removed }: TurnOutcomes): string {
    const registeredCount = built.length + updated.length;

    if (failed.length > 0) {
        return `Built ${registeredCount} of ${formatCount(registeredCount + failed.length, "collection")}`;
    }

    if (updated.length > 0) {
        return `Updated ${formatCount(registeredCount, "collection")}`;
    }

    if (built.length > 0) {
        return `Built ${formatCount(built.length, "collection")}`;
    }

    if (removed.length > 0) {
        return `Removed ${formatCount(removed.length, "collection")}`;
    }

    if (kept.length > 0) {
        return `Kept ${formatCount(kept.length, "collection")} as built`;
    }

    return "Updated the plan";
}

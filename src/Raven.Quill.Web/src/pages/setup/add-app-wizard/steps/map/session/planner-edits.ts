import type { CdcSinkTableConfig, MigrationPlanChange, ValidationIssue } from "@/api/generated/server-api";
import type { PlannerEdit, PlannerView } from "@/pages/setup/add-app-wizard/steps/map/session/planner-types";

/** Save issues the server could not place in one collection, such as a plan that changed meanwhile. */
export const WHOLE_PLAN_ISSUES = "";

export type PlanChanges = {
    changes: MigrationPlanChange[];
    /** The collection each change is about, as the plan named it before the save. */
    collections: string[];
};

/**
 * Turns the operator's edits into one plan/save batch. A collection is edited first, under the name the plan
 * has for it, then renamed: the rename finds the collection where the edit left it.
 */
export function buildPlanChanges(edits: Record<string, PlannerEdit>): PlanChanges {
    const changes: MigrationPlanChange[] = [];
    const collections: string[] = [];
    const add = (collection: string, change: MigrationPlanChange) => {
        changes.push(change);
        collections.push(collection);
    };

    for (const [collection, edit] of Object.entries(edits)) {
        if (edit.isRemoved) {
            add(collection, { kind: "Delete", area: edit.area, collection });
            continue;
        }

        const newName = getEditedName(collection, edit);
        const isRenamed = newName !== collection;

        if (edit.config) {
            add(collection, {
                kind: "Edit",
                area: edit.area,
                collection,
                config: { ...edit.config, collectionName: collection },
            });
        }

        if (isRenamed) {
            add(collection, { kind: "Rename", area: edit.area, collection, newName });
        }
    }

    return { changes, collections };
}

export function getSavedView(collection: string, edit: PlannerEdit): PlannerView {
    return { area: edit.area, collection: getEditedName(collection, edit) };
}

/** Applies a change on top of a collection's pending edit, or drops the edit once nothing is left in it. */
export function patchEdit(
    area: string,
    edit: PlannerEdit | null | undefined,
    change: Partial<Omit<PlannerEdit, "area">>,
): PlannerEdit | null {
    const patched: PlannerEdit = {
        area,
        config: edit?.config ?? null,
        isRemoved: edit?.isRemoved ?? false,
        ...change,
    };

    return patched.config || patched.isRemoved ? patched : null;
}

function getEditedName(collection: string, edit: PlannerEdit): string {
    return edit.config?.collectionName?.trim() || collection;
}

/** Sorts the issues of a refused save by collection, each path made relative to that collection's mapping. */
export function groupSaveIssues(issues: ValidationIssue[], collections: string[]): Record<string, ValidationIssue[]> {
    const grouped: Record<string, ValidationIssue[]> = {};

    for (const issue of issues) {
        const match = issue.path?.match(/^changes\[(\d+)\]\.?(.*)$/);
        const collection = match ? (collections[Number(match[1])] ?? WHOLE_PLAN_ISSUES) : WHOLE_PLAN_ISSUES;
        const path = match ? match[2] || null : (issue.path ?? null);

        grouped[collection] = [...(grouped[collection] ?? []), { ...issue, path }];
    }

    return grouped;
}

/**
 * Where an issue path points in the editor's form, which holds a mapping as its one table and keeps string
 * lists as { value } items: `embeddedTables[0].joinColumns[1]` is `mapTables.tables.0.embeddedTables.0.joinColumns.1.value`.
 */
export function toEditorFieldPath(path: string | null | undefined): string | null {
    if (!path) {
        return null;
    }

    const formPath = path
        .replace(/\[(\d+)\]/g, ".$1")
        .replace(/(primaryKeyColumns|joinColumns)\.(\d+)$/, "$1.$2.value");

    return `mapTables.tables.0.${formPath}`;
}

/** Two mappings are the same edit when they serialize alike; the form adds nothing a save would not send. */
export function isSameConfig(left: CdcSinkTableConfig | null, right: CdcSinkTableConfig | null): boolean {
    return JSON.stringify(left) === JSON.stringify(right);
}

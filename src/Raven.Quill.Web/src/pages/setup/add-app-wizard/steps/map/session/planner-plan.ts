import type { CdcSinkTableConfig, DiscoverTableResponse } from "@/api/generated/server-api";
import { getTableKey, getTableLabel } from "@/pages/setup/add-app-wizard/discover-utils";
import {
    documentFromConfig,
    documentFromProposal,
    getEmbeddedTables,
    getLinkedCollections,
    getLinkTargets,
    type DiscoverIndex,
    type LinkTargets,
    type PlanDocument,
} from "@/pages/setup/add-app-wizard/steps/map/session/planner-document";
import { isRegistered, type PlannerState } from "@/pages/setup/add-app-wizard/steps/map/session/planner-state";
import type {
    PlannerCollection,
    PlannerProposal,
    ProposedCollection,
    SelectedTables,
} from "@/pages/setup/add-app-wizard/steps/map/session/planner-types";

export type PlanCollectionState = "suggested" | "built" | "updated" | "failed";

export const PLAN_STATE_LABELS: Record<PlanCollectionState, string> = {
    suggested: "Suggested",
    built: "Built",
    updated: "Updated",
    failed: "Failed",
};

export type PlanCollection = {
    name: string;
    state: PlanCollectionState;
    area: string | null;
    why: string | null;
    rootTable: string | null;
    document: PlanDocument | null;
    previousDocument: PlanDocument | null;
    config: CdcSinkTableConfig | null;
    warnings: string[];
    errors: string[];
    /** The collection's name and its tables, as the name filter looks for them. */
    searchKey: string;
};

export type PlanArea = { name: string | null; collections: PlanCollection[] };

export type PlanTableGroup = "notMigrated" | "notSelected" | "inCollections";

/** Tables without a collection come first, so nothing gets forgotten. */
export const PLAN_TABLE_GROUP_LABELS: Record<PlanTableGroup, string> = {
    notMigrated: "Not migrated",
    notSelected: "Not selected",
    inCollections: "In collections",
};

export type PlanTable = { label: string; detail: string; searchKey: string } & (
    | { group: "inCollections"; collection: PlanCollection }
    | { group: "notMigrated" | "notSelected"; collection: null }
);

/** What the plan is made of: the latest proposal, what the planner registered, and what was taken out since. */
export type PlanSource = Pick<PlannerState, "plannerProposal" | "plannerCollections" | "plannerRemovedCollections">;

/**
 * A collection is rebuilt only when something it is made of changed, so a frame about one collection
 * leaves every other collection - and whatever renders it - as it was.
 */
const planCollections = new WeakMap<object, { inputs: unknown[]; collection: PlanCollection }>();

/**
 * One list for the whole plan: every proposed collection in the proposal's order, followed by any the
 * planner registered without proposing it. Registering a collection is what moves it past Suggested.
 */
export function buildPlanCollections(source: PlanSource, index: DiscoverIndex): PlanCollection[] {
    const proposal = source.plannerProposal;
    const proposedByName = new Map(
        (proposal?.collections ?? []).flatMap((proposed) =>
            proposed.collection ? [[proposed.collection, proposed] as const] : [],
        ),
    );
    const areaByName = getAreaByName(proposal);
    const linkTargets = getLinkTargets(proposal?.collections ?? [], index);

    return getPlanCollectionNames(source).map((name) => {
        const proposed = proposedByName.get(name) ?? null;
        const registered: PlannerCollection | null = source.plannerCollections[name] ?? null;
        const area = areaByName.get(name) ?? null;

        return reusePlanCollection(registered ?? proposed, [name, proposed, registered, area, index], () =>
            toPlanCollection(name, proposed, registered, area, index, linkTargets),
        );
    });
}

export function getRegisteredState(version: number): "built" | "updated" {
    return version > 1 ? "updated" : "built";
}

export function isBuilt(collection: PlanCollection): boolean {
    return collection.state === "built" || collection.state === "updated";
}

export function countByState(collections: PlanCollection[]): Record<PlanCollectionState, number> {
    const counts: Record<PlanCollectionState, number> = { suggested: 0, built: 0, updated: 0, failed: 0 };

    for (const collection of collections) {
        counts[collection.state] += 1;
    }

    return counts;
}

export function countByGroup(tables: PlanTable[]): Record<PlanTableGroup, number> {
    const counts: Record<PlanTableGroup, number> = { notMigrated: 0, notSelected: 0, inCollections: 0 };

    for (const table of tables) {
        counts[table.group] += 1;
    }

    return counts;
}

export function countUnbuilt(source: PlanSource): number {
    return getPlanCollectionNames(source).filter((name) => {
        const collection = source.plannerCollections[name];

        return !collection || !isRegistered(collection);
    }).length;
}

/**
 * The other collections that link to `target`; without it in the plan, those links point at nothing. A
 * link names its collection the way the configuration was written, so the case is not taken as given.
 */
export function findCollectionsLinkingTo(collections: PlanCollection[], target: string): string[] {
    const isTarget = (name: string) => name.toLowerCase() === target.toLowerCase();

    return collections
        .filter(
            (collection) =>
                !isTarget(collection.name) &&
                collection.document !== null &&
                getLinkedCollections(collection.document).some(isTarget),
        )
        .map((collection) => collection.name);
}

export function getMemberTables(collection: Pick<PlanCollection, "document" | "rootTable">): string[] {
    if (!collection.document) {
        return collection.rootTable ? [collection.rootTable] : [];
    }

    return [collection.document.table, ...getEmbeddedTables(collection.document).map((embedded) => embedded.table)];
}

/** Areas in the proposal's order. Collections the proposal never placed come last, outside any area. */
export function groupByArea(collections: PlanCollection[], proposal: PlannerProposal | null): PlanArea[] {
    const areaNames = [...new Set(getAreaByName(proposal).values())];

    return [...areaNames, null]
        .map((name) => ({ name, collections: collections.filter((collection) => collection.area === name) }))
        .filter((area) => area.collections.length > 0);
}

export function buildPlanTables(
    index: DiscoverIndex,
    selectedTables: SelectedTables,
    collections: PlanCollection[],
    proposal: PlannerProposal | null,
): PlanTable[] {
    const selectedKeys = new Set(selectedTables.map(getTableKey));
    const membership = new Map<DiscoverTableResponse, { collection: PlanCollection; path: string | null }>();
    const droppedWhy = new Map<DiscoverTableResponse, string>();

    for (const collection of collections) {
        if (!collection.document) {
            continue;
        }

        const members = [{ source: collection.document.source, path: null }, ...getEmbeddedTables(collection.document)];

        for (const { source, path } of members) {
            const table = index.find(source);

            if (table && !membership.has(table)) {
                membership.set(table, { collection, path });
            }
        }
    }

    for (const dropped of proposal?.dropped ?? []) {
        const table = index.find(dropped.tableRef);

        if (table && dropped.why) {
            droppedWhy.set(table, dropped.why);
        }
    }

    return index.tables.map((table): PlanTable => {
        const label = getTableLabel(table);
        const member = membership.get(table);

        if (member) {
            return {
                label,
                searchKey: toSearchKey(label, member.collection.name),
                group: "inCollections",
                detail: member.path
                    ? `Inside ${member.collection.name} as ${member.path}`
                    : `One ${member.collection.name} document per row`,
                collection: member.collection,
            };
        }

        const searchKey = toSearchKey(label);

        if (selectedKeys.has(getTableKey(table))) {
            return {
                label,
                searchKey,
                group: "notMigrated",
                detail: droppedWhy.get(table) ?? "Not in any collection.",
                collection: null,
            };
        }

        return {
            label,
            searchKey,
            group: "notSelected",
            detail: table.unsupportedReason ?? "Not selected in Verify your schema.",
            collection: null,
        };
    });
}

/** What each collection looked like, to tell later which of them changed. */
export function getPlanRevisions(source: PlanSource): Record<string, string> {
    return Object.fromEntries(
        getPlanCollectionNames(source).map((name) => [name, getRevision(source.plannerCollections[name])]),
    );
}

export function countChangedSince(revisions: Record<string, string>, source: PlanSource): number {
    return getPlanCollectionNames(source).filter(
        (name) => revisions[name] !== getRevision(source.plannerCollections[name]),
    ).length;
}

export function toSearchKey(...names: string[]): string {
    return names.join("\n").toLowerCase();
}

/** Matches a search key holding the filter, ignoring case and surrounding spaces. A blank filter matches all. */
export function createNameMatcher(filter: string): (searchKey: string) => boolean {
    const wanted = filter.trim().toLowerCase();

    return (searchKey) => !wanted || searchKey.includes(wanted);
}

function getPlanCollectionNames(source: PlanSource): string[] {
    const removedNames = new Set(source.plannerRemovedCollections);
    const proposedNames = (source.plannerProposal?.collections ?? []).flatMap(({ collection }) =>
        collection && !removedNames.has(collection) ? [collection] : [],
    );

    return [...new Set([...proposedNames, ...Object.keys(source.plannerCollections)])];
}

/** The area each proposed collection belongs to. A blank area name places it in no area. */
function getAreaByName(proposal: PlannerProposal | null): Map<string, string> {
    const areaByName = new Map<string, string>();

    for (const { area, collections } of proposal?.areas ?? []) {
        const areaName = area?.trim();

        if (!areaName) {
            continue;
        }

        for (const collection of collections) {
            if (!areaByName.has(collection)) {
                areaByName.set(collection, areaName);
            }
        }
    }

    return areaByName;
}

function reusePlanCollection(key: object | null, inputs: unknown[], build: () => PlanCollection): PlanCollection {
    const cached = key ? planCollections.get(key) : undefined;

    if (cached?.inputs.every((input, position) => input === inputs[position])) {
        return cached.collection;
    }

    const collection = build();

    if (key) {
        planCollections.set(key, { inputs, collection });
    }

    return collection;
}

function toPlanCollection(
    name: string,
    proposed: ProposedCollection | null,
    registered: PlannerCollection | null,
    area: string | null,
    index: DiscoverIndex,
    linkTargets: LinkTargets,
): PlanCollection {
    const getProposedDocument = () => (proposed ? documentFromProposal(proposed, index, linkTargets) : null);
    const withSearchKey = (collection: Omit<PlanCollection, "searchKey">): PlanCollection => ({
        ...collection,
        searchKey: toSearchKey(collection.name, ...getMemberTables(collection)),
    });

    if (registered && isRegistered(registered)) {
        const document = registered.config
            ? documentFromConfig(registered.config, registered.tableRef)
            : getProposedDocument();

        return withSearchKey({
            name,
            state: getRegisteredState(registered.version),
            area,
            why: registered.rationale ?? proposed?.why ?? null,
            rootTable: document?.table ?? proposed?.rootTable ?? null,
            document,
            previousDocument: registered.previousConfig ? documentFromConfig(registered.previousConfig, null) : null,
            config: registered.config,
            warnings: registered.warnings,
            errors: registered.rejectionErrors,
        });
    }

    const document = getProposedDocument();

    return withSearchKey({
        name,
        state: registered ? "failed" : "suggested",
        area,
        why: proposed?.why ?? null,
        rootTable: document?.table ?? proposed?.rootTable ?? null,
        document,
        previousDocument: null,
        config: null,
        warnings: [],
        errors: registered?.errors ?? [],
    });
}

function getRevision(collection: PlannerCollection | undefined): string {
    if (!collection) {
        return "suggested";
    }

    return isRegistered(collection)
        ? `built:${collection.registrationNumber}:${collection.rejectionErrors.length}`
        : `failed:${collection.errors.length}`;
}

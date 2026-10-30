import type { CdcSinkTableConfig } from "@/api/generated/server-api";
import { getTableKey, getTableLabel } from "@/pages/setup/add-app-wizard/discover-utils";
import {
    documentFromConfig,
    documentFromProposal,
    getEmbeddedTables,
    getLinkTargets,
    type DiscoverIndex,
    type LinkTargets,
    type PlanDocument,
} from "@/pages/setup/add-app-wizard/steps/map/session/planner-document";
import {
    isAreaStarted,
    isChatWorking,
    selectChat,
    type PlannerState,
} from "@/pages/setup/add-app-wizard/steps/map/session/planner-state";
import { resolveConfigTables } from "@/pages/setup/add-app-wizard/steps/map/session/planner-table-refs";
import type {
    PlannerEdit,
    PlannerEntry,
    ProposedCollection,
} from "@/pages/setup/add-app-wizard/steps/map/session/planner-types";

export type PlanCollectionState = "suggested" | "building" | "built" | "updated" | "failed" | "removed";

export const PLAN_STATE_LABELS: Record<PlanCollectionState, string> = {
    suggested: "Not built",
    building: "Building",
    built: "Built",
    updated: "Updated",
    failed: "Failed",
    removed: "Removed",
};

export type PlanCollection = {
    name: string;
    area: string;
    state: PlanCollectionState;
    why: string | null;
    rootTable: string | null;
    /** The mapping as built, or as the operator edited it; the proposal's outline before anything is built. */
    document: PlanDocument | null;
    config: CdcSinkTableConfig | null;
    warnings: string[];
    /** Why the agent rejected the collection, or its latest change. */
    problems: string[];
    retryErrors: string[];
    edit: PlannerEdit | null;
    searchKey: string;
};

export type PlanAreaState = "notStarted" | "building" | "partlyBuilt" | "built" | "failed";

export type PlanArea = {
    name: string;
    why: string | null;
    state: PlanAreaState;
    isStarted: boolean;
    isBuilding: boolean;
    collections: PlanCollection[];
    /** Proposed collections nothing was built for yet: what Build still has to do. */
    pendingCollections: string[];
};

export type CollectionSourceKind = "table" | "embed" | "link";

/** `as` is the property an embedded table lands under. */
export type CollectionSource = { kind: CollectionSourceKind; name: string; as: string | null };

export type CollectionSources = {
    table: CollectionSource | null;
    embeds: CollectionSource[];
    links: CollectionSource[];
};

export type PlanLeftOutTable = { table: string; why: string; searchKey: string };

export type Plan = { areas: PlanArea[]; collections: PlanCollection[]; leftOut: PlanLeftOutTable[] };

export type PlanSource = Pick<
    PlannerState,
    | "plannerProposal"
    | "plannerEntries"
    | "plannerRetrying"
    | "plannerRejectionIssues"
    | "plannerStartedAreas"
    | "plannerProposalChat"
    | "plannerAreaChats"
    | "plannerEdits"
    | "plannerSelectedTables"
    | "plannerDefaultSchema"
>;

const proposedDocuments = new WeakMap<ProposedCollection, PlanDocument | null>();

/**
 * The plan as the views show it: the proposal's areas in its order, each with the collections it holds
 * now. A stored collection decides where it is - a move takes it to another area - and one the proposal
 * names but the plan renamed shows once, under its new name.
 */
export function buildPlan(source: PlanSource, index: DiscoverIndex): Plan {
    const proposal = source.plannerProposal;
    const proposedNames = new Set(
        proposal?.areas.flatMap((area) => area.collections.flatMap(({ collection }) => collection ?? [])) ?? [],
    );
    const linkTargets = getLinkTargets(proposal?.areas.flatMap((area) => area.collections) ?? [], index);
    const renamedRootTables = new Set(
        Object.values(source.plannerEntries).flatMap((entry) =>
            !proposedNames.has(entry.name) && entry.tableRef?.table ? [getTableKey(entry.tableRef.table)] : [],
        ),
    );

    const areaNames = [
        ...new Set([
            ...(proposal?.areas.flatMap((area) => area.area?.trim() || []) ?? []),
            ...Object.values(source.plannerEntries).flatMap((entry) => entry.area ?? []),
        ]),
    ];

    const areas = areaNames.map((areaName): PlanArea => {
        const proposed = proposal?.areas.find((area) => area.area?.trim() === areaName);
        const isBuilding = isChatWorking(selectChat(source, areaName));
        const isStarted = isAreaStarted(source, areaName);

        const proposedHere = (proposed?.collections ?? []).filter((collection) => {
            const name = collection.collection;

            if (!name) {
                return false;
            }

            const entry: PlannerEntry | undefined = source.plannerEntries[name];

            if (entry) {
                return entry.area === null || entry.area === areaName;
            }

            return !collection.rootTableRef || !renamedRootTables.has(getTableKey(collection.rootTableRef));
        });
        const storedHere = Object.values(source.plannerEntries).filter(
            (entry) => entry.area === areaName && !proposedHere.some(({ collection }) => collection === entry.name),
        );

        const collections = [
            ...proposedHere.map((proposedCollection) =>
                toPlanCollection(
                    proposedCollection.collection!,
                    areaName,
                    proposedCollection,
                    source,
                    index,
                    linkTargets,
                    isBuilding,
                ),
            ),
            ...storedHere.map((entry) =>
                toPlanCollection(entry.name, areaName, null, source, index, linkTargets, isBuilding),
            ),
        ];

        return {
            name: areaName,
            why: proposed?.why ?? null,
            state: getAreaState(collections, isStarted, isBuilding),
            isStarted,
            isBuilding,
            collections,
            pendingCollections: proposedHere
                .filter(({ collection }) => !source.plannerEntries[collection!])
                .map(({ collection }) => collection!),
        };
    });

    const collections = areas.flatMap((area) => area.collections);

    return { areas, collections, leftOut: buildLeftOutTables(source, index, collections) };
}

export function isBuilt(collection: Pick<PlanCollection, "state">): boolean {
    return collection.state === "built" || collection.state === "updated";
}

/** A collection the agent mapped, so it has a page with its mapping, and can be edited by hand. */
export function isMapped(collection: Pick<PlanCollection, "state" | "config">): boolean {
    return (isBuilt(collection) || collection.state === "failed") && collection.config !== null;
}

export function countBuilt(collections: PlanCollection[]): number {
    return collections.filter(isBuilt).length;
}

export function toSearchKey(...names: string[]): string {
    return names.join("\n").toLowerCase();
}

/** Matches a search key holding the filter, ignoring case and surrounding spaces. A blank filter matches all. */
export function createNameMatcher(filter: string): (searchKey: string) => boolean {
    const wanted = filter.trim().toLowerCase();

    return (searchKey) => !wanted || searchKey.includes(wanted);
}

export function getMemberTables(collection: Pick<PlanCollection, "document" | "rootTable">): string[] {
    if (!collection.document) {
        return collection.rootTable ? [collection.rootTable] : [];
    }

    return [collection.document.table, ...getEmbeddedTables(collection.document).map((embedded) => embedded.table)];
}

/**
 * Where a collection's documents come from: its table, the tables it embeds, and every collection it links to.
 * Only a built mapping names the property an embedded table lands under; the proposal has no names yet.
 */
export function getCollectionSources(
    collection: Pick<PlanCollection, "document" | "rootTable" | "config">,
): CollectionSources {
    const { document } = collection;

    if (!document) {
        return {
            table: collection.rootTable ? { kind: "table", name: collection.rootTable, as: null } : null,
            embeds: [],
            links: [],
        };
    }

    const embeds = document.properties.filter((property) => property.kind === "embed");

    return {
        table: { kind: "table", name: document.table, as: null },
        embeds: embeds.map((embed) => ({
            kind: "embed",
            name: embed.document.table,
            as: collection.config ? embed.name : null,
        })),
        links: [...new Set(getLinkedCollections(document))].map((name) => ({ kind: "link", name, as: null })),
    };
}

/** The area's name without the word area, for the plan, where the areas already sit under an Areas heading. */
export function getAreaShortName(area: string): string {
    return area.replace(/\s+area$/i, "") || area;
}

function toPlanCollection(
    name: string,
    area: string,
    proposed: ProposedCollection | null,
    source: PlanSource,
    index: DiscoverIndex,
    linkTargets: LinkTargets,
    isAreaBuilding: boolean,
): PlanCollection {
    const entry: PlannerEntry | undefined = source.plannerEntries[name];
    const edit = source.plannerEdits[name] ?? null;
    const config = edit?.config ?? entry?.config ?? null;
    const document = getDocument(config, entry, edit, proposed, source, index, linkTargets);
    const retryErrors = source.plannerRetrying[name] ?? [];
    const rejection = source.plannerRejectionIssues[name]?.map((issue) => issue.message);
    const isRejected = entry?.status === "Rejected";
    const collection = {
        name,
        area,
        state: getCollectionState(entry, edit, isAreaBuilding, retryErrors.length > 0),
        why: entry?.rationale ?? proposed?.why ?? null,
        rootTable: document?.table ?? proposed?.rootTable ?? null,
        document,
        config,
        warnings: isRejected ? [] : (entry?.warnings ?? []),
        problems: rejection ?? (isRejected ? (entry?.warnings ?? []) : []),
        retryErrors,
        edit,
    };

    return { ...collection, searchKey: toSearchKey(name, ...getMemberTables(collection)) };
}

/** A hand edit reads like the agent's own: an edited collection is Updated, a fixed failure Built. */
function getCollectionState(
    entry: PlannerEntry | undefined,
    edit: PlannerEdit | null,
    isAreaBuilding: boolean,
    isRetrying: boolean,
): PlanCollectionState {
    if (isAreaBuilding && (!entry || isRetrying)) {
        return "building";
    }

    if (!entry) {
        return "suggested";
    }

    switch (entry.status) {
        case "Removed":
            return "removed";
        case "Rejected":
            return edit?.config ? "built" : "failed";
        case "Registered":
            return edit?.config || entry.version > 1 ? "updated" : "built";
    }
}

function getDocument(
    config: CdcSinkTableConfig | null,
    entry: PlannerEntry | undefined,
    edit: PlannerEdit | null,
    proposed: ProposedCollection | null,
    source: PlanSource,
    index: DiscoverIndex,
    linkTargets: LinkTargets,
): PlanDocument | null {
    if (config) {
        const tableRef =
            edit?.config || !entry
                ? resolveConfigTables(config, source.plannerSelectedTables ?? [], source.plannerDefaultSchema)
                : entry.tableRef;

        return documentFromConfig(config, tableRef);
    }

    if (!proposed) {
        return null;
    }

    if (!proposedDocuments.has(proposed)) {
        proposedDocuments.set(proposed, documentFromProposal(proposed, index, linkTargets));
    }

    return proposedDocuments.get(proposed) ?? null;
}

function getAreaState(collections: PlanCollection[], isStarted: boolean, isBuilding: boolean): PlanAreaState {
    if (isBuilding) {
        return "building";
    }

    if (collections.some((collection) => collection.state === "failed")) {
        return "failed";
    }

    const builtCount = countBuilt(collections);

    if (!isStarted && builtCount === 0) {
        return "notStarted";
    }

    return collections.every((collection) => isBuilt(collection) || collection.state === "removed")
        ? "built"
        : "partlyBuilt";
}

function buildLeftOutTables(
    source: PlanSource,
    index: DiscoverIndex,
    collections: PlanCollection[],
): PlanLeftOutTable[] {
    const covered = new Set(
        collections
            .filter((collection) => collection.state !== "removed")
            .flatMap((collection) => getCoveredTableKeys(collection)),
    );
    const droppedWhy = new Map(
        (source.plannerProposal?.dropped ?? []).flatMap((dropped) =>
            dropped.tableRef && dropped.why ? [[getTableKey(dropped.tableRef), dropped.why] as const] : [],
        ),
    );

    return (source.plannerSelectedTables ?? [])
        .filter((table) => !covered.has(getTableKey(table)))
        .map((table) => {
            const label = getTableLabel(index.find(table) ?? table);

            return {
                table: label,
                why: droppedWhy.get(getTableKey(table)) ?? "Not in any collection.",
                searchKey: toSearchKey(label),
            };
        })
        .toSorted((left, right) => left.table.localeCompare(right.table));
}

function getLinkedCollections(document: PlanDocument): string[] {
    return document.properties.flatMap((property) => {
        switch (property.kind) {
            case "link":
                return [property.collection];
            case "embed":
                return getLinkedCollections(property.document);
            default:
                return [];
        }
    });
}

function getCoveredTableKeys(collection: PlanCollection): string[] {
    if (!collection.document) {
        return [];
    }

    const sources = [
        collection.document.source,
        ...getEmbeddedTables(collection.document).map((table) => table.source),
    ];

    return sources.flatMap((table) => (table ? [getTableKey(table)] : []));
}

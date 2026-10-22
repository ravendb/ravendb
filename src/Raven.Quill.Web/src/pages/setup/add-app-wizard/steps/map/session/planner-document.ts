import type {
    CdcSinkEmbeddedTableConfig,
    CdcSinkLinkedTableConfig,
    CdcSinkRelationType,
    CdcSinkTableConfig,
    DiscoverResponse,
    DiscoverTableResponse,
} from "@/api/generated/server-api";
import { getTableKey, getTableLabel, type SourceTableRef } from "@/pages/setup/add-app-wizard/discover-utils";
import { getForeignKeysToTable } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-utils";
import type { ConfigTableRef, ProposedCollection } from "@/pages/setup/add-app-wizard/steps/map/session/planner-types";

/** `source` is the selected table the document reads from, or null when its name fits none. */
export type PlanDocument = { table: string; source: SourceTableRef | null; properties: PlanProperty[] };

export type PlanColumnProperty = { kind: "column"; name: string; column: string; table: string };

export type PlanLinkProperty = { kind: "link"; name: string; collection: string; joinColumns: string[] };

export type PlanEmbedProperty = {
    kind: "embed";
    name: string;
    relation: CdcSinkRelationType;
    joinColumns: string[];
    document: PlanDocument;
};

export type PlanProperty = PlanColumnProperty | PlanLinkProperty | PlanEmbedProperty;

export type PlanEmbeddedTable = { table: string; source: SourceTableRef | null; path: string };

export type PlanShapeRow = { property: PlanProperty; path: string; depth: number; isNew: boolean };

/** The discovered tables, looked up by name once instead of scanned for every reference. */
export type DiscoverIndex = {
    /** Sorted by label. */
    tables: DiscoverTableResponse[];
    /** Takes a resolved selected table, so it is spelled exactly as discovered. */
    find: (table: SourceTableRef | null | undefined) => DiscoverTableResponse | undefined;
};

/** The collection whose documents are read from each table, for naming what a link points at. */
export type LinkTargets = Map<DiscoverTableResponse, string>;

const discoverIndexes = new WeakMap<DiscoverResponse, DiscoverIndex>();

const EMPTY_INDEX = createDiscoverIndex([]);

const configDocuments = new WeakMap<CdcSinkTableConfig, { tableRef: ConfigTableRef | null; document: PlanDocument }>();

export function indexDiscovery(discoverResult: DiscoverResponse | null): DiscoverIndex {
    if (!discoverResult) {
        return EMPTY_INDEX;
    }

    const cached = discoverIndexes.get(discoverResult);

    if (cached) {
        return cached;
    }

    const index = createDiscoverIndex(discoverResult.tables);
    discoverIndexes.set(discoverResult, index);

    return index;
}

export function documentFromConfig(config: CdcSinkTableConfig, tableRef: ConfigTableRef | null): PlanDocument {
    const cached = configDocuments.get(config);

    if (cached?.tableRef === tableRef) {
        return cached.document;
    }

    const document = toConfigDocument(config, null, tableRef);
    configDocuments.set(config, { tableRef, document });

    return document;
}

/**
 * The document a suggested collection would produce. The proposal names tables only, so the properties
 * carry their source names: the root table's columns, embedded tables under their own names, and links
 * under the column that holds the reference. Columns a relation consumes are not repeated as properties.
 */
export function documentFromProposal(
    proposed: ProposedCollection,
    index: DiscoverIndex,
    linkTargets: LinkTargets,
): PlanDocument | null {
    const root = index.find(proposed.rootTableRef);

    if (!root) {
        return null;
    }

    // What the planner absorbs in some other way than a link still lands inside the document, once per
    // table: the planner may list a table twice, or absorb the root it already reads from.
    const placedTables = new Set([root]);
    const absorbed = proposed.absorbs.flatMap((absorbs) => {
        const table = index.find(absorbs.tableRef);

        if (!table || placedTables.has(table)) {
            return [];
        }

        placedTables.add(table);
        return [{ table, isLink: absorbs.how?.trim().toLowerCase() === "link" }];
    });

    const rootNode = createNode(root, []);
    const nodes = [rootNode];
    let pendingEmbeds = absorbed.filter((item) => !item.isLink).map((item) => item.table);

    // An embedded table hangs under whichever placed table it references, so the lines of an order land
    // in the order even though the proposal lists every absorbed table flat.
    while (pendingEmbeds.length > 0) {
        const placements = pendingEmbeds.flatMap((table) => {
            const parent = findReference(nodes, (node) => getForeignKeysToTable(table, node.table));
            return parent ? [{ table, ...parent }] : [];
        });

        if (placements.length === 0) {
            break;
        }

        for (const { table, node, joinColumns } of placements) {
            const child = createNode(table, joinColumns);
            node.embeds.push(child);
            nodes.push(child);
        }

        pendingEmbeds = pendingEmbeds.filter((table) => !placements.some((placement) => placement.table === table));
    }

    rootNode.embeds.push(...pendingEmbeds.map((table) => createNode(table, [])));

    for (const { table } of absorbed.filter((item) => item.isLink)) {
        const reference = findReference(nodes, (node) => getForeignKeysToTable(node.table, table));
        const joinColumns = reference?.joinColumns ?? [];

        (reference?.node ?? rootNode).links.push({
            kind: "link",
            name: joinColumns.length === 1 ? joinColumns[0] : table.sourceTableName,
            collection: linkTargets.get(table) ?? table.sourceTableName,
            joinColumns,
        });
    }

    return toProposalDocument(rootNode);
}

export function getLinkTargets(proposed: ProposedCollection[], index: DiscoverIndex): LinkTargets {
    const targets: LinkTargets = new Map();

    for (const { collection, rootTableRef } of proposed) {
        const root = index.find(rootTableRef);

        if (collection && root && !targets.has(root)) {
            targets.set(root, collection);
        }
    }

    return targets;
}

export function getEmbeddedTables(document: PlanDocument, parentPath = ""): PlanEmbeddedTable[] {
    return document.properties.flatMap((property) => {
        if (property.kind !== "embed") {
            return [];
        }

        const path = joinPath(parentPath, property.name);
        const { table, source } = property.document;

        return [{ table, source, path }, ...getEmbeddedTables(property.document, path)];
    });
}

export function getLinkedCollections(document: PlanDocument): string[] {
    return document.properties.flatMap((property) => {
        if (property.kind === "link") {
            return [property.collection];
        }

        return property.kind === "embed" ? getLinkedCollections(property.document) : [];
    });
}

/** The document as table rows, nested properties under their parent. `previous` marks what is new. */
export function getShapeRows(document: PlanDocument, previous: PlanDocument | null): PlanShapeRow[] {
    const previousPaths = previous ? new Set(flattenProperties(previous, "", 0).map((row) => row.path)) : null;

    return flattenProperties(document, "", 0).map((row) => ({
        ...row,
        isNew: previousPaths !== null && !previousPaths.has(row.path),
    }));
}

function createDiscoverIndex(tables: DiscoverTableResponse[]): DiscoverIndex {
    const byKey = new Map(tables.map((table) => [getTableKey(table), table]));

    return {
        tables: tables.toSorted((left, right) => getTableLabel(left).localeCompare(getTableLabel(right))),
        find: (table) => (table ? byKey.get(getTableKey(table)) : undefined),
    };
}

function flattenProperties(document: PlanDocument, parentPath: string, depth: number): Omit<PlanShapeRow, "isNew">[] {
    return document.properties.flatMap((property) => {
        const path = joinPath(parentPath, property.name);
        const row = { property, path, depth };

        return property.kind === "embed" ? [row, ...flattenProperties(property.document, path, depth + 1)] : [row];
    });
}

function joinPath(parentPath: string, name: string): string {
    return parentPath ? `${parentPath}.${name}` : name;
}

/** The planner leaves an embedded table's schema out when it matches the parent's, as the server reads it too. */
function toConfigDocument(
    table: CdcSinkTableConfig | CdcSinkEmbeddedTableConfig,
    parentSchema: string | null,
    tableRef: ConfigTableRef | null,
): PlanDocument {
    const schema = table.sourceTableSchema || parentSchema;
    const label = getTableLabel({ sourceTableSchema: schema, sourceTableName: table.sourceTableName });
    const linkedTables: CdcSinkLinkedTableConfig[] = table.linkedTables ?? [];

    return {
        table: label,
        source: tableRef?.table ?? null,
        properties: [
            ...(table.columns ?? []).map(
                (column): PlanProperty => ({
                    kind: "column",
                    name: column.name ?? "",
                    column: column.column ?? "",
                    table: label,
                }),
            ),
            ...linkedTables.map(
                (linked): PlanProperty => ({
                    kind: "link",
                    name: linked.propertyName ?? "",
                    collection: linked.linkedCollectionName ?? "",
                    joinColumns: linked.joinColumns ?? [],
                }),
            ),
            ...(table.embeddedTables ?? []).map(
                (embedded, position): PlanProperty => ({
                    kind: "embed",
                    name: embedded.propertyName ?? "",
                    relation: embedded.type ?? "Array",
                    joinColumns: embedded.joinColumns ?? [],
                    document: toConfigDocument(embedded, schema, tableRef?.embeddedTables[position] ?? null),
                }),
            ),
        ],
    };
}

type ProposalNode = {
    table: DiscoverTableResponse;
    /** The columns pointing at the parent table, which embedding the table consumes. */
    joinColumns: string[];
    embeds: ProposalNode[];
    links: PlanLinkProperty[];
};

function createNode(table: DiscoverTableResponse, joinColumns: string[]): ProposalNode {
    return { table, joinColumns, embeds: [], links: [] };
}

function findReference(
    nodes: ProposalNode[],
    getForeignKeys: (node: ProposalNode) => DiscoverTableResponse["foreignKeys"],
): { node: ProposalNode; joinColumns: string[] } | undefined {
    for (const node of nodes) {
        const [foreignKey] = getForeignKeys(node);

        if (foreignKey) {
            return { node, joinColumns: foreignKey.columns };
        }
    }

    return undefined;
}

function toProposalDocument(node: ProposalNode): PlanDocument {
    const label = getTableLabel(node.table);
    const consumedColumns = new Set([...node.joinColumns, ...node.links.flatMap((link) => link.joinColumns)]);

    return {
        table: label,
        source: {
            sourceTableSchema: node.table.sourceTableSchema ?? null,
            sourceTableName: node.table.sourceTableName,
        },
        properties: [
            ...node.table.columns
                .filter((column) => !consumedColumns.has(column.name))
                .map(
                    (column): PlanProperty => ({
                        kind: "column",
                        name: column.name,
                        column: column.name,
                        table: label,
                    }),
                ),
            ...node.links,
            ...node.embeds.map(
                (embed): PlanProperty => ({
                    kind: "embed",
                    name: embed.table.sourceTableName,
                    relation: "Array",
                    joinColumns: embed.joinColumns,
                    document: toProposalDocument(embed),
                }),
            ),
        ],
    };
}

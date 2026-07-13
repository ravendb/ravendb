import {
    castToEmbeddedTablePath,
    castToLinkedTablePath,
    getRootTablePath,
    type EmbeddedTablePath,
    type ExplorerRow,
    type FormEmbeddedTable,
    type FormRootTable,
    type RootTablePath,
} from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-types";

const NO_SCHEMA_LABEL = "(no schema)";

type ExpandedPaths = Record<string, boolean>;

type BuildExplorerRowsArgs = {
    tables: FormRootTable[];
    expandedPaths: ExpandedPaths;
    filter: string;
};

/** Flattens the configured tables into explorer rows: root tables grouped by source
 * schema, with linked/embedded children included for expanded tables. */
export function buildExplorerRows({ tables, expandedPaths, filter }: BuildExplorerRowsArgs): ExplorerRow[] {
    const normalizedFilter = filter.trim().toLowerCase();

    const indexedTables = tables.map((table, index) => ({ table, index }));
    const filteredTables = normalizedFilter
        ? indexedTables.filter(({ table }) => (table?.sourceTableName ?? "").toLowerCase().includes(normalizedFilter))
        : indexedTables;

    const groupedBySchema = new Map<string, typeof filteredTables>();
    for (const entry of filteredTables) {
        const schema = entry.table?.sourceTableSchema || NO_SCHEMA_LABEL;
        const group = groupedBySchema.get(schema) ?? [];
        group.push(entry);
        groupedBySchema.set(schema, group);
    }

    const rows: ExplorerRow[] = [];

    for (const [schema, groupTables] of groupedBySchema) {
        rows.push({ type: "schema", rowKey: `schema:${schema}`, label: schema });

        for (const { table, index } of groupTables) {
            const path = getRootTablePath(index);
            const isExpanded = Boolean(expandedPaths[path]);

            rows.push({
                type: "root",
                path,
                rowKey: path,
                table,
                hasChildren: hasChildTables(table),
                isExpanded,
            });

            if (isExpanded) {
                addChildRows({
                    rows,
                    parentPath: path,
                    table,
                    isRootDisabled: Boolean(table?.disabled),
                    depth: 1,
                    expandedPaths,
                });
            }
        }
    }

    return rows;
}

type AddChildRowsArgs = {
    rows: ExplorerRow[];
    parentPath: RootTablePath | EmbeddedTablePath;
    table: FormRootTable | FormEmbeddedTable;
    isRootDisabled: boolean;
    depth: number;
    expandedPaths: ExpandedPaths;
};

function addChildRows({ rows, parentPath, table, isRootDisabled, depth, expandedPaths }: AddChildRowsArgs) {
    table?.linkedTables?.forEach((linkedTable, idx) => {
        const path = castToLinkedTablePath(`${parentPath}.linkedTables.${idx}`);

        rows.push({
            type: "linked",
            path,
            rowKey: path,
            table: linkedTable,
            isRootDisabled,
            depth,
        });
    });

    table?.embeddedTables?.forEach((embeddedTable, idx) => {
        const path = castToEmbeddedTablePath(`${parentPath}.embeddedTables.${idx}`);
        const isExpanded = Boolean(expandedPaths[path]);

        rows.push({
            type: "embedded",
            path,
            rowKey: path,
            table: embeddedTable,
            hasChildren: hasChildTables(embeddedTable),
            isExpanded,
            isRootDisabled,
            depth,
        });

        if (isExpanded) {
            addChildRows({
                rows,
                parentPath: path,
                table: embeddedTable,
                isRootDisabled,
                depth: depth + 1,
                expandedPaths,
            });
        }
    });
}

function hasChildTables(table: FormRootTable | FormEmbeddedTable) {
    return Boolean(table?.linkedTables?.length || table?.embeddedTables?.length);
}

import { useVirtualizer } from "@tanstack/react-virtual";
import { EmptySet } from "components/common/EmptySet";
import { useAppSelector } from "components/store";
import {
    castToEmbeddedTablePath,
    castToLinkedTablePath,
    ExplorerRow,
    FormEmbeddedTable,
    FormRootTable,
    getRootTablePath,
} from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskTypes";
import { EditCdcSinkTaskFormData } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskValidation";
import _ from "lodash";
import { useMemo, useRef } from "react";
import { FieldPath, useFormContext, useWatch } from "react-hook-form";
import { EditCdcSinkTaskRootTableItem } from "./EditCdcSinkTaskRootTableItem";
import { EditCdcSinkTaskLinkedTableItem } from "./EditCdcSinkTaskLinkedTableItem";
import { EditCdcSinkTaskEmbeddedTableItem } from "./EditCdcSinkTaskEmbeddedTableItem";
import assertUnreachable from "components/utils/assertUnreachable";
import { editCdcSinkTaskConstants } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskConstants";
import {
    analyzeRootTables,
    getEmbeddedTableWarningMessagesFromAnalysis,
    getLinkedTableWarningMessagesFromAnalysis,
    getRootTableWarningMessagesFromAnalysis,
    RootTablesAnalysis,
} from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskTableWarnings";

const { explorerRowHeightPx } = editCdcSinkTaskConstants;

type ExpandedTables = Partial<Record<FieldPath<EditCdcSinkTaskFormData>, boolean>>;

interface EditCdcSinkTaskTableItemsProps {
    filter: string;
    rootFieldIds: string[];
}

interface BuildExplorerRowsArgs {
    allTables: FormRootTable[];
    expandedTables: ExpandedTables;
    filter: string;
    rootFieldIds: string[];
}

interface IndexedRootTable {
    index: number;
    table: FormRootTable;
}

export function EditCdcSinkTaskTableItems({ filter, rootFieldIds }: EditCdcSinkTaskTableItemsProps) {
    const parentRef = useRef<HTMLDivElement>(null);
    const expandedTables = useAppSelector((state) => state.editCdcSinkTask.expandedTables);
    const { control } = useFormContext<EditCdcSinkTaskFormData>();
    const allTables = useWatch({ control, name: "tables" }) ?? [];

    const filteredTables = useMemo(
        () =>
            buildExplorerRows({
                allTables,
                expandedTables,
                filter,
                rootFieldIds,
            }),
        [filter, expandedTables, allTables, rootFieldIds]
    );

    const virtualizer = useVirtualizer({
        count: filteredTables.length,
        getScrollElement: () => parentRef.current,
        estimateSize: () => explorerRowHeightPx,
        getItemKey: (index) => filteredTables[index].rowKey,
        overscan: 4,
    });
    const virtualRows = virtualizer.getVirtualItems();
    const activeSchemaLabel = getActiveSchemaLabel(filteredTables, virtualizer.scrollOffset ?? 0);

    if (allTables.length === 0) {
        return <EmptySet compact>Use the Schema Explorer to discover existing tables or add new manually.</EmptySet>;
    }

    if (filteredTables.length === 0) {
        return <EmptySet compact>No tables match the filter.</EmptySet>;
    }

    return (
        <div ref={parentRef} className="overflow-y-auto flex-grow-1 min-height-0">
            <div
                className="position-sticky top-0 z-1"
                style={{ height: explorerRowHeightPx, marginBottom: -explorerRowHeightPx }}
            >
                <SchemaRow label={activeSchemaLabel} />
            </div>
            <div style={{ height: `${virtualizer.getTotalSize()}px`, position: "relative" }}>
                {virtualRows.map((virtualRow) => {
                    const row = filteredTables[virtualRow.index];

                    return (
                        <div
                            key={virtualRow.key}
                            data-index={virtualRow.index}
                            style={{
                                position: "absolute",
                                top: 0,
                                left: 0,
                                width: "100%",
                                height: `${virtualRow.size}px`,
                                transform: `translateY(${virtualRow.start}px)`,
                            }}
                            className="virtual-item explorer-row-item"
                        >
                            <ExplorerRowItem row={row} />
                        </div>
                    );
                })}
            </div>
        </div>
    );
}

export function buildExplorerRows({ allTables, expandedTables, filter, rootFieldIds }: BuildExplorerRowsArgs) {
    const normalizedFilter = filter.trim().toLowerCase();
    const rootTablesAnalysis = analyzeRootTables(allTables);

    const indexedTables: IndexedRootTable[] = allTables.map((table, index) => ({
        table,
        index,
    }));

    const filteredGroupedTables = Object.entries(
        _.groupBy(indexedTables, ({ table }) => table?.sourceTableSchema || "public")
    )
        .map(([schema, rootTables]) => ({
            schema,
            tables: normalizedFilter
                ? rootTables.filter(({ table }) =>
                      (table?.sourceTableName ?? "").toLowerCase().includes(normalizedFilter)
                  )
                : rootTables,
        }))
        .filter((group) => group.tables.length > 0);

    return filteredGroupedTables.flatMap(({ schema, tables }) => {
        const rows: ExplorerRow[] = [
            {
                type: "schema",
                path: `schema:${schema}`,
                rowKey: `schema:${schema}`,
                label: schema,
            },
        ];

        tables.forEach(({ table, index }) => {
            const rootPath = getRootTablePath(index);
            const rootRowKey = getRootRowKey(rootFieldIds, index, rootPath);
            const isExpanded = Boolean(expandedTables[rootPath]);
            const warningMessages = getRootTableWarningMessagesFromAnalysis(rootTablesAnalysis, table);

            rows.push({
                type: "root",
                path: rootPath,
                rowKey: rootRowKey,
                warningMessages,
                table,
                hasChildren: hasChildren(table),
                isExpanded,
            });

            if (isExpanded) {
                addChildRows({
                    rows,
                    parentPath: rootPath,
                    rowKeyPrefix: rootRowKey,
                    table,
                    isRootDisabled: Boolean(table?.disabled),
                    depth: 1,
                    expandedTables,
                    rootTablesAnalysis,
                });
            }
        });

        return rows;
    });
}

function ExplorerRowItem({ row }: { row: ExplorerRow }) {
    const rowType = row.type;
    switch (rowType) {
        case "schema":
            return <SchemaRow label={row.label} />;
        case "root":
            return <EditCdcSinkTaskRootTableItem {...row} />;
        case "linked":
            return <EditCdcSinkTaskLinkedTableItem {...row} />;
        case "embedded":
            return <EditCdcSinkTaskEmbeddedTableItem {...row} />;
        default:
            assertUnreachable(rowType);
    }
}

function SchemaRow({ label }: { label: string }) {
    return (
        <div className="text-center font-monospace small panel-bg-2" style={{ height: explorerRowHeightPx }}>
            {label}
        </div>
    );
}

function getActiveSchemaLabel(rows: ExplorerRow[], scrollOffset: number): string {
    const firstVisibleIndex = Math.min(rows.length - 1, Math.floor(scrollOffset / explorerRowHeightPx));

    for (let index = firstVisibleIndex; index >= 0; index--) {
        const row = rows[index];

        if (row.type === "schema") {
            return row.label;
        }
    }

    return "";
}

interface AddChildRowsArgs {
    rows: ExplorerRow[];
    parentPath: string;
    rowKeyPrefix: string;
    table: FormRootTable | FormEmbeddedTable;
    isRootDisabled: boolean;
    depth: number;
    expandedTables: ExpandedTables;
    rootTablesAnalysis: RootTablesAnalysis;
}

function addChildRows({
    rows,
    parentPath,
    rowKeyPrefix,
    table,
    isRootDisabled,
    depth,
    expandedTables,
    rootTablesAnalysis,
}: AddChildRowsArgs) {
    table?.linkedTables?.forEach((linkedTable, idx) => {
        const path = castToLinkedTablePath(`${parentPath}.linkedTables.${idx}`);
        const warningMessages = getLinkedTableWarningMessagesFromAnalysis(rootTablesAnalysis, linkedTable);
        rows.push({
            type: "linked",
            path,
            rowKey: `${rowKeyPrefix}:linked:${idx}`,
            warningMessages,
            table: linkedTable,
            isRootDisabled,
            depth,
        });
    });

    table?.embeddedTables?.forEach((embeddedTable, idx) => {
        const path = castToEmbeddedTablePath(`${parentPath}.embeddedTables.${idx}`);
        const rowKey = `${rowKeyPrefix}:embedded:${idx}`;
        const isExpanded = Boolean(expandedTables[path]);
        const warningMessages = getEmbeddedTableWarningMessagesFromAnalysis(rootTablesAnalysis, embeddedTable);
        rows.push({
            type: "embedded",
            path,
            rowKey,
            warningMessages,
            table: embeddedTable,
            isRootDisabled,
            depth,
            hasChildren: hasChildren(embeddedTable),
            isExpanded,
        });

        if (isExpanded) {
            addChildRows({
                rows,
                parentPath: path,
                rowKeyPrefix: rowKey,
                table: embeddedTable,
                isRootDisabled,
                depth: depth + 1,
                expandedTables,
                rootTablesAnalysis,
            });
        }
    });
}

function hasChildren(table: FormRootTable | FormEmbeddedTable) {
    return Boolean(table?.linkedTables?.length || table?.embeddedTables?.length);
}

function getRootRowKey(rootFieldIds: string[], index: number, rootPath: ReturnType<typeof getRootTablePath>) {
    return `root:${rootFieldIds[index] ?? rootPath}`;
}

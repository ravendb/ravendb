import React, { useMemo, useState } from "react";
import {
    ColumnDef,
    ExpandedState,
    Row,
    getCoreRowModel,
    getExpandedRowModel,
    getFilteredRowModel,
    getSortedRowModel,
    useReactTable,
} from "@tanstack/react-table";
import { Icon } from "components/common/Icon";
import NodeTagPill from "./NodeTagPill";
import { ExpandIndicator, NodeTagPillStack, expandableRowProps } from "./nodeStackTable";
import VirtualTable from "components/common/virtualTable/VirtualTable";
import { virtualTableUtils } from "components/common/virtualTable/utils/virtualTableUtils";
import { analyzerConstants } from "./analyzerConstants";
import SummaryBar from "./SummaryBar";
import SizeGetter from "components/common/SizeGetter";

type DebugPackageAnalysisSummary = Raven.Server.Documents.Handlers.Debugging.DebugPackage.DebugPackageAnalysisSummary;

interface DatabasesOverviewProps {
    summary: DebugPackageAnalysisSummary;
}

interface DatabasesOverviewWithSizeProps extends DatabasesOverviewProps {
    width: number;
}

interface TableRow {
    rowKind: "database" | "node";
    database: string;
    nodeTag?: string;
    documentsCount?: number;
    indexesCount?: number;
    erroredIndexesCount?: number;
    indexingErrorsCount?: number;
    ongoingTasksCount?: number;
    replicationFactor?: number;
    disabled?: boolean;
    nodes?: { nodeTag: string; disabled: boolean }[];
    subRows?: TableRow[];
}

function useDatabasesOverviewColumns(availableWidth: number) {
    const bodyWidth = virtualTableUtils.getTableBodyWidth(availableWidth);
    const getSize = virtualTableUtils.getCellSizeProvider(bodyWidth);

    const databasesColumns: ColumnDef<TableRow>[] = useMemo(
        () => [
            {
                header: "Database",
                accessorKey: "database",
                cell: DbNameCell,
                size: getSize(20),
            },
            {
                header: "Node",
                accessorKey: "nodeTag",
                cell: DbNodeTagCell,
                size: getSize(8),
            },
            {
                header: "Documents",
                accessorKey: "documentsCount",
                cell: DbDocumentsCell,
                size: getSize(12),
            },
            {
                header: "Indexes",
                accessorKey: "indexesCount",
                cell: DbIndexesCell,
                size: getSize(11),
            },
            {
                header: "Indexing errors",
                accessorKey: "indexingErrorsCount",
                cell: DbIndexingErrorsCell,
                size: getSize(14),
            },
            {
                header: "Ongoing tasks",
                accessorKey: "ongoingTasksCount",
                cell: DbOngoingTasksCell,
                size: getSize(14),
            },
            {
                header: "Replication factor",
                accessorKey: "replicationFactor",
                cell: DbReplicationFactorCell,
                size: getSize(14),
            },
            {
                header: "State",
                id: "state",
                accessorFn: (row) => row.disabled,
                cell: DbStateCell,
                size: getSize(7),
            },
        ],
        [getSize]
    );

    return { databasesColumns };
}

export default function DatabasesOverview({ summary }: DatabasesOverviewProps) {
    return <SizeGetter render={({ width }) => <DatabasesOverviewWithSize summary={summary} width={width} />} />;
}

function DatabasesOverviewWithSize({ summary, width }: DatabasesOverviewWithSizeProps) {
    const rows = useMemo(() => buildTableRows(summary), [summary]);
    const [expanded, setExpanded] = useState<ExpandedState>({});

    // every top-level row is a database; its nodes live in subRows and are revealed on expand
    const disabledCount = rows.filter((r) => r.disabled).length;
    const onlineCount = rows.length - disabledCount;

    const { databasesColumns } = useDatabasesOverviewColumns(width);

    const table = useReactTable({
        data: rows,
        columns: databasesColumns,
        state: { expanded },
        onExpandedChange: setExpanded,
        getSubRows: (row) => row.subRows,
        getRowCanExpand: (row) => (row.original.subRows?.length ?? 0) > 0,
        enableSorting: rows.length > analyzerConstants.minRowsForControls,
        enableColumnFilters: rows.length > analyzerConstants.minRowsForControls,
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
        getExpandedRowModel: getExpandedRowModel(),
        getRowId: (row) => (row.nodeTag ? `${row.database}/${row.nodeTag}` : row.database),
    });

    const heightInPx = virtualTableUtils.getHeightInPx(table.getRowModel().rows.length, 400);

    return (
        <div className="databases-overview">
            <div className="panel-bg-1 rounded">
                <div className="p-4 vstack gap-3">
                    <h3 className="mb-0">Databases Overview</h3>
                    <SummaryBar
                        items={[
                            { icon: "database", count: rows.length, label: "total" },
                            {
                                icon: "database",
                                iconAddon: "check",
                                count: onlineCount,
                                label: "online",
                                colorClass: "text-success",
                            },
                            {
                                icon: "database",
                                iconAddon: "cancel",
                                count: disabledCount,
                                label: "disabled",
                                colorClass: "text-warning",
                            },
                        ]}
                    />

                    {rows.length === 0 ? (
                        <div className="text-muted text-center w-100">No databases found in the package</div>
                    ) : (
                        <VirtualTable table={table} heightInPx={heightInPx} {...expandableRowProps<TableRow>()} />
                    )}
                </div>
            </div>
        </div>
    );
}

function DbNameCell({ row }: { row: Row<TableRow> }) {
    if (row.original.rowKind !== "database") {
        return null;
    }
    return (
        <span className="hstack gap-1 fw-bold">
            {row.getCanExpand() && <ExpandIndicator expanded={row.getIsExpanded()} />}
            {row.original.database}
        </span>
    );
}

function DbNodeTagCell({ row }: { row: Row<TableRow> }) {
    if (row.original.rowKind === "node") {
        return <NodeTagPill tag={row.original.nodeTag!} />;
    }

    const nodes = row.original.nodes ?? [];
    if (nodes.length === 0) {
        return null;
    }

    return <NodeTagPillStack tags={nodes.map((node) => node.nodeTag)} />;
}

function DbDocumentsCell({ row }: { row: { original: TableRow } }) {
    return row.original.rowKind === "database" ? formatCount(row.original.documentsCount ?? -1) : null;
}

function DbIndexesCell({ row }: { row: { original: TableRow } }) {
    if (row.original.rowKind !== "database") {
        return null;
    }
    const errored = row.original.erroredIndexesCount ?? 0;
    return (
        <>
            {formatCount(row.original.indexesCount ?? -1)}
            {errored > 0 && (
                <span className="text-danger ms-1">
                    <Icon icon="danger" margin="m-0" /> {errored}
                </span>
            )}
        </>
    );
}

function DbIndexingErrorsCell({ row }: { row: { original: TableRow } }) {
    if (row.original.rowKind !== "database") {
        return null;
    }
    const count = row.original.indexingErrorsCount ?? 0;
    return <span className={count > 0 ? "text-danger" : ""}>{formatCount(count)}</span>;
}

function DbOngoingTasksCell({ row }: { row: { original: TableRow } }) {
    return row.original.rowKind === "database" ? formatCount(row.original.ongoingTasksCount ?? -1) : null;
}

function DbReplicationFactorCell({ row }: { row: { original: TableRow } }) {
    return row.original.rowKind === "database" ? formatCount(row.original.replicationFactor ?? -1) : null;
}

function DbStateCell({ row }: { row: Row<TableRow> }) {
    if (row.original.rowKind === "node") {
        return <StateLabel disabled={row.original.disabled} />;
    }

    // collapsed database row: summarize the per-node state so it isn't blank when collapsed
    const nodes = row.original.nodes ?? [];
    const disabledCount = nodes.filter((node) => node.disabled).length;

    if (disabledCount === 0 || disabledCount === nodes.length) {
        return <StateLabel disabled={disabledCount > 0} />;
    }

    return (
        <span className="hstack gap-1 text-warning">
            <Icon icon="database" addon="cancel" margin="m-0" /> {nodes.length - disabledCount}/{nodes.length} online
        </span>
    );
}

function StateLabel({ disabled }: { disabled: boolean }) {
    return disabled ? (
        <span className="hstack gap-1 text-warning">
            <Icon icon="database" addon="cancel" margin="m-0" /> Disabled
        </span>
    ) : (
        <span className="hstack gap-1 text-success">
            <Icon icon="database" addon="check" margin="m-0" /> Online
        </span>
    );
}

function formatCount(value: number): string {
    if (value == null || value < 0) {
        return "-";
    }
    return value.toLocaleString();
}

function buildTableRows(summary: DebugPackageAnalysisSummary): TableRow[] {
    const dbMap = new Map<
        string,
        {
            documentsCount: number;
            indexesCount: number;
            erroredIndexesCount: number;
            indexingErrorsCount: number;
            ongoingTasksCount: number;
            replicationFactor: number;
            disabled: boolean;
            nodes: { nodeTag: string; disabled: boolean }[];
        }
    >();

    Object.entries(summary.SummaryPerNode ?? {}).forEach(([nodeTag, node]) => {
        (node.DatabasesOverview?.Items ?? []).forEach((item) => {
            if (item.Irrelevant) {
                return;
            }

            let agg = dbMap.get(item.Database);
            if (!agg) {
                agg = {
                    documentsCount: item.DocumentsCount,
                    indexesCount: item.IndexesCount,
                    erroredIndexesCount: item.ErroredIndexesCount,
                    indexingErrorsCount: item.IndexingErrorsCount,
                    ongoingTasksCount: item.OngoingTasksCount,
                    replicationFactor: item.ReplicationFactor,
                    disabled: item.Disabled,
                    nodes: [],
                };
                dbMap.set(item.Database, agg);
            } else {
                agg.documentsCount = Math.max(agg.documentsCount, item.DocumentsCount);
                agg.indexesCount = Math.max(agg.indexesCount, item.IndexesCount);
                agg.erroredIndexesCount = Math.max(agg.erroredIndexesCount, item.ErroredIndexesCount);
                agg.indexingErrorsCount = Math.max(agg.indexingErrorsCount, item.IndexingErrorsCount);
                agg.ongoingTasksCount = Math.max(agg.ongoingTasksCount, item.OngoingTasksCount);
                agg.replicationFactor = Math.max(agg.replicationFactor, item.ReplicationFactor);
            }

            if (!agg.nodes.some((n) => n.nodeTag === nodeTag)) {
                agg.nodes.push({ nodeTag, disabled: item.Disabled });
            }
        });
    });

    const result: TableRow[] = [];

    [...dbMap.keys()]
        .sort((a, b) => a.localeCompare(b))
        .forEach((database) => {
            const agg = dbMap.get(database)!;
            const sortedNodes = [...agg.nodes].sort((a, b) => a.nodeTag.localeCompare(b.nodeTag));

            result.push({
                rowKind: "database",
                database,
                documentsCount: agg.documentsCount,
                indexesCount: agg.indexesCount,
                erroredIndexesCount: agg.erroredIndexesCount,
                indexingErrorsCount: agg.indexingErrorsCount,
                ongoingTasksCount: agg.ongoingTasksCount,
                replicationFactor: agg.replicationFactor,
                disabled: agg.disabled,
                nodes: sortedNodes,
                subRows: sortedNodes.map(
                    ({ nodeTag, disabled }): TableRow => ({ rowKind: "node", database, nodeTag, disabled })
                ),
            });
        });

    return result;
}

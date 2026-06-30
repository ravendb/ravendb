import React, { useMemo } from "react";
import {
    ColumnDef,
    Row,
    getCoreRowModel,
    getExpandedRowModel,
    getFilteredRowModel,
    getSortedRowModel,
    useReactTable,
} from "@tanstack/react-table";
import NodeTagPill from "./NodeTagPill";
import { ExpandIndicator, NodeTagPillStack, RangeCell, canExpandNodeRow, expandableRowProps } from "./NodeStackTable";
import { MetricRange, toReplicaRange } from "./analyzerUtils";
import { useExpandAllSync } from "./ExpandAllContext";
import { EmptySet } from "components/common/EmptySet";
import genUtils from "common/generalUtils";
import VirtualTable from "components/common/virtualTable/VirtualTable";
import { virtualTableUtils } from "components/common/virtualTable/utils/virtualTableUtils";
import { analyzerConstants } from "./analyzerConstants";
import SizeGetter from "components/common/SizeGetter";

type DebugPackageAnalysisSummary = Raven.Server.Documents.Handlers.Debugging.DebugPackage.DebugPackageAnalysisSummary;

const formatBytes = (value: number): string => genUtils.formatBytesToSize(value);

interface StorageTableRow {
    rowKind: "database" | "node";
    key: string;
    database: string;
    nodeTag?: string;
    // per-node rows carry a single value (min === max); database rows carry the replica spread
    size: MetricRange;
    temp: MetricRange;
    total: MetricRange;
    nodeTags?: string[];
    subRows?: StorageTableRow[];
}

interface StoragePerDatabaseProps {
    summary: DebugPackageAnalysisSummary;
    nodeTag?: string;
}

interface StoragePerDatabaseWithSizeProps extends StoragePerDatabaseProps {
    width: number;
}

function useStorageColumns(availableWidth: number) {
    const bodyWidth = virtualTableUtils.getTableBodyWidth(
        availableWidth - analyzerConstants.panelHorizontalPaddingInPx
    );
    const getSize = useMemo(() => virtualTableUtils.getCellSizeProvider(bodyWidth), [bodyWidth]);

    const storageColumns: ColumnDef<StorageTableRow>[] = useMemo(
        () => [
            {
                header: "Database",
                accessorKey: "database",
                cell: StorageDbNameCell,
                size: getSize(36),
            },
            {
                header: "Node",
                accessorKey: "nodeTag",
                cell: StorageNodeTagCell,
                size: getSize(14),
            },
            {
                header: "Data",
                id: "size",
                accessorFn: (row) => row.size.max,
                cell: ({ row }) => <RangeCell range={row.original.size} format={formatBytes} />,
                size: getSize(17),
            },
            {
                header: "Temp",
                id: "temp",
                accessorFn: (row) => row.temp.max,
                cell: ({ row }) => <RangeCell range={row.original.temp} format={formatBytes} />,
                size: getSize(17),
            },
            {
                header: "Total",
                id: "total",
                accessorFn: (row) => row.total.max,
                cell: ({ row }) => <RangeCell range={row.original.total} format={formatBytes} />,
                size: getSize(16),
            },
        ],
        [getSize]
    );

    return { storageColumns };
}

export default function StoragePerDatabase({ summary, nodeTag }: StoragePerDatabaseProps) {
    return (
        <SizeGetter
            render={({ width }) => <StoragePerDatabaseWithSize summary={summary} nodeTag={nodeTag} width={width} />}
        />
    );
}

function StoragePerDatabaseWithSize({ summary, nodeTag, width }: StoragePerDatabaseWithSizeProps) {
    const rows = useMemo(() => buildStorageRows(summary, nodeTag), [summary, nodeTag]);
    const [expanded, setExpanded] = useExpandAllSync();

    const { storageColumns } = useStorageColumns(width);

    const table = useReactTable({
        data: rows,
        columns: storageColumns,
        state: { expanded },
        onExpandedChange: setExpanded,
        getSubRows: (row) => row.subRows,
        getRowCanExpand: canExpandNodeRow,
        enableSorting: rows.length > analyzerConstants.minRowsForControls,
        enableColumnFilters: rows.length > analyzerConstants.minRowsForControls,
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
        getExpandedRowModel: getExpandedRowModel(),
        getRowId: (row) => row.key,
    });

    const heightInPx = virtualTableUtils.getHeightInPx(table.getRowModel().rows.length, 400);

    return (
        <div className="flex-grow-1">
            <div className="panel-bg-1 rounded">
                <div className="p-4">
                    <h3 className="mb-3">Storage per Database</h3>
                    {rows.length === 0 ? (
                        <EmptySet compact className="justify-content-center">
                            No storage data in the package
                        </EmptySet>
                    ) : (
                        <VirtualTable
                            table={table}
                            heightInPx={heightInPx}
                            {...expandableRowProps<StorageTableRow>()}
                        />
                    )}
                </div>
            </div>
        </div>
    );
}

function StorageDbNameCell({ row }: { row: Row<StorageTableRow> }) {
    if (row.original.rowKind !== "database") {
        return null;
    }
    return (
        <span className="hstack gap-1">
            {row.getCanExpand() && <ExpandIndicator expanded={row.getIsExpanded()} />}
            {row.original.database}
        </span>
    );
}

function StorageNodeTagCell({ row }: { row: Row<StorageTableRow> }) {
    if (row.original.rowKind === "node") {
        return <NodeTagPill tag={row.original.nodeTag!} />;
    }
    const tags = row.original.nodeTags ?? [];
    return tags.length > 0 ? <NodeTagPillStack tags={tags} /> : null;
}

export function buildStorageRows(summary: DebugPackageAnalysisSummary, nodeTag?: string): StorageTableRow[] {
    const dbMap = new Map<string, { nodes: { nodeTag: string; size: number; temp: number }[] }>();

    Object.entries(summary.SummaryPerNode ?? {}).forEach(([tag, node]) => {
        if (nodeTag && tag !== nodeTag) {
            return;
        }

        (node.DatabaseStorageUsage?.Items ?? []).forEach((item) => {
            let agg = dbMap.get(item.Database);
            if (!agg) {
                agg = { nodes: [] };
                dbMap.set(item.Database, agg);
            }
            agg.nodes.push({ nodeTag: tag, size: item.Size, temp: item.TempBuffersSize });
        });
    });

    const result: StorageTableRow[] = [];

    [...dbMap.keys()]
        .sort((a, b) => a.localeCompare(b))
        .forEach((database) => {
            const agg = dbMap.get(database)!;

            const sortedNodes = [...agg.nodes].sort((a, b) => a.nodeTag.localeCompare(b.nodeTag));

            result.push({
                rowKind: "database",
                key: database,
                database,
                // the children are replicas of the same data, so we show the spread, never the sum
                size: toReplicaRange(sortedNodes.map((n) => n.size)),
                temp: toReplicaRange(sortedNodes.map((n) => n.temp)),
                total: toReplicaRange(sortedNodes.map((n) => n.size + n.temp)),
                nodeTags: sortedNodes.map((n) => n.nodeTag),
                subRows: sortedNodes.map(
                    ({ nodeTag: tag, size, temp }): StorageTableRow => ({
                        rowKind: "node",
                        key: `${database}/${tag}`,
                        database,
                        nodeTag: tag,
                        size: toReplicaRange([size]),
                        temp: toReplicaRange([temp]),
                        total: toReplicaRange([size + temp]),
                    })
                ),
            });
        });

    return result;
}

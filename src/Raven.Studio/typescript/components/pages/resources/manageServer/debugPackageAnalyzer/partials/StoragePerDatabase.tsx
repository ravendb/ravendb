import React, { useMemo } from "react";
import {
    ColumnDef,
    getCoreRowModel,
    getFilteredRowModel,
    getSortedRowModel,
    useReactTable,
} from "@tanstack/react-table";
import NodeTagPill from "./NodeTagPill";
import { EmptySet } from "components/common/EmptySet";
import genUtils from "common/generalUtils";
import VirtualTable from "components/common/virtualTable/VirtualTable";
import { virtualTableUtils } from "components/common/virtualTable/utils/virtualTableUtils";
import { analyzerConstants } from "./analyzerConstants";
import SizeGetter from "components/common/SizeGetter";

type DebugPackageAnalysisSummary = Raven.Server.Documents.Handlers.Debugging.DebugPackage.DebugPackageAnalysisSummary;

interface StorageTableRow {
    rowKind: "database" | "node";
    key: string;
    database: string;
    nodeTag?: string;
    size: number;
    temp: number;
}

interface StoragePerDatabaseProps {
    summary: DebugPackageAnalysisSummary;
    nodeTag?: string;
}

interface StoragePerDatabaseWithSizeProps extends StoragePerDatabaseProps {
    width: number;
}

function useStorageColumns(availableWidth: number) {
    const bodyWidth = virtualTableUtils.getTableBodyWidth(availableWidth);
    const getSize = virtualTableUtils.getCellSizeProvider(bodyWidth);

    const storageColumns: ColumnDef<StorageTableRow>[] = useMemo(
        () => [
            {
                header: "Database",
                accessorKey: "database",
                cell: storageDbNameCell,
                size: getSize(36),
            },
            {
                header: "Node",
                accessorKey: "nodeTag",
                cell: storageNodeTagCell,
                size: getSize(14),
            },
            {
                header: "Data",
                accessorKey: "size",
                cell: ({ getValue }) => genUtils.formatBytesToSize(getValue<number>()),
                size: getSize(17),
            },
            {
                header: "Temp",
                accessorKey: "temp",
                cell: ({ getValue }) => genUtils.formatBytesToSize(getValue<number>()),
                size: getSize(17),
            },
            {
                header: "Total",
                id: "total",
                accessorFn: (row) => row.size + row.temp,
                cell: ({ getValue }) => genUtils.formatBytesToSize(getValue<number>()),
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
    const databaseRows = rows.filter((r) => r.rowKind === "database");

    const { storageColumns } = useStorageColumns(width);

    const table = useReactTable({
        data: rows,
        columns: storageColumns,
        enableSorting: rows.length > analyzerConstants.minRowsForControls,
        enableColumnFilters: rows.length > analyzerConstants.minRowsForControls,
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
        getRowId: (row) => row.key,
    });

    const heightInPx = virtualTableUtils.getHeightInPx(rows.length, 400);

    return (
        <div className="storage-per-database flex-grow-1">
            <div className="panel-bg-1 rounded">
                <div className="p-4">
                    <h3 className="mb-3">Storage per Database</h3>
                    {databaseRows.length === 0 ? (
                        <EmptySet compact className="justify-content-center">
                            No storage data in the package
                        </EmptySet>
                    ) : (
                        <VirtualTable table={table} heightInPx={heightInPx} />
                    )}
                </div>
            </div>
        </div>
    );
}

function storageDbNameCell({ row }: { row: { original: StorageTableRow } }) {
    return row.original.rowKind === "database" ? <span className="fw-bold">{row.original.database}</span> : null;
}

function storageNodeTagCell({ row }: { row: { original: StorageTableRow } }) {
    return row.original.rowKind === "node" ? <NodeTagPill tag={row.original.nodeTag!} /> : null;
}

function buildStorageRows(summary: DebugPackageAnalysisSummary, nodeTag?: string): StorageTableRow[] {
    const dbMap = new Map<
        string,
        {
            totalSize: number;
            totalTemp: number;
            nodes: { nodeTag: string; size: number; temp: number }[];
        }
    >();

    Object.entries(summary.SummaryPerNode ?? {}).forEach(([tag, node]) => {
        if (nodeTag && tag !== nodeTag) return;

        (node.DatabaseStorageUsage?.Items ?? []).forEach((item) => {
            let agg = dbMap.get(item.Database);
            if (!agg) {
                agg = { totalSize: 0, totalTemp: 0, nodes: [] };
                dbMap.set(item.Database, agg);
            }
            agg.totalSize += item.Size;
            agg.totalTemp += item.TempBuffersSize;
            agg.nodes.push({ nodeTag: tag, size: item.Size, temp: item.TempBuffersSize });
        });
    });

    const result: StorageTableRow[] = [];

    [...dbMap.keys()]
        .sort((a, b) => a.localeCompare(b))
        .forEach((database) => {
            const agg = dbMap.get(database)!;

            result.push({
                rowKind: "database",
                key: database,
                database,
                size: agg.totalSize,
                temp: agg.totalTemp,
            });

            agg.nodes
                .sort((a, b) => a.nodeTag.localeCompare(b.nodeTag))
                .forEach(({ nodeTag: tag, size, temp }) => {
                    result.push({
                        rowKind: "node",
                        key: `${database}/${tag}`,
                        database,
                        nodeTag: tag,
                        size,
                        temp,
                    });
                });
        });

    return result;
}

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
import VirtualTable from "components/common/virtualTable/VirtualTable";
import { virtualTableUtils } from "components/common/virtualTable/utils/virtualTableUtils";
import { analyzerConstants } from "./analyzerConstants";
import SizeGetter from "components/common/SizeGetter";

type DebugPackageAnalysisSummary = Raven.Server.Documents.Handlers.Debugging.DebugPackage.DebugPackageAnalysisSummary;

interface IndexingRow {
    node: string;
    indexed: number;
    mapped: number;
    reduced: number;
}

interface IndexingPerNodeProps {
    summary: DebugPackageAnalysisSummary;
    nodeTag?: string;
}

interface IndexingPerNodeWithSizeProps extends IndexingPerNodeProps {
    width: number;
}

function useIndexingPerNodeColumns(availableWidth: number) {
    const bodyWidth = virtualTableUtils.getTableBodyWidth(
        availableWidth - analyzerConstants.panelHorizontalPaddingInPx
    );
    const getSize = virtualTableUtils.getCellSizeProvider(bodyWidth);

    const indexingColumns: ColumnDef<IndexingRow>[] = useMemo(
        () => [
            {
                header: "Node",
                accessorKey: "node",
                cell: ({ getValue }) => <NodeTagPill tag={getValue<string>()} />,
                size: getSize(18),
                enableSorting: true,
            },
            {
                header: "Indexed/s",
                accessorKey: "indexed",
                cell: ({ getValue }) => formatRate(getValue<number>()),
                size: getSize(27),
                enableSorting: true,
            },
            {
                header: "Mapped/s",
                accessorKey: "mapped",
                cell: ({ getValue }) => formatRate(getValue<number>()),
                size: getSize(27),
                enableSorting: true,
            },
            {
                header: "Reduced/s",
                accessorKey: "reduced",
                cell: ({ getValue }) => formatRate(getValue<number>()),
                size: getSize(28),
                enableSorting: true,
            },
        ],
        [getSize]
    );

    return { indexingColumns };
}

export default function IndexingPerNode({ summary, nodeTag }: IndexingPerNodeProps) {
    return (
        <SizeGetter
            render={({ width }) => <IndexingPerNodeWithSize summary={summary} nodeTag={nodeTag} width={width} />}
        />
    );
}

function IndexingPerNodeWithSize({ summary, nodeTag, width }: IndexingPerNodeWithSizeProps) {
    const rows = useMemo(() => collectIndexingRows(summary, nodeTag), [summary, nodeTag]);

    const { indexingColumns } = useIndexingPerNodeColumns(width);

    const table = useReactTable({
        data: rows,
        columns: indexingColumns,
        enableSorting: rows.length > analyzerConstants.minRowsForControls,
        enableColumnFilters: rows.length > analyzerConstants.minRowsForControls,
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
        initialState: {
            sorting: [{ id: "node", desc: false }],
        },
        getRowId: (row) => row.node,
    });

    const heightInPx = virtualTableUtils.getHeightInPx(rows.length, 400);

    return (
        <div className="indexing-per-node flex-grow-1">
            <div className="panel-bg-1 rounded">
                <div className="p-4">
                    <h3 className="mb-3">Indexing per Node</h3>
                    {rows.length === 0 ? (
                        <EmptySet compact className="justify-content-center">
                            No indexing data in the package
                        </EmptySet>
                    ) : (
                        <VirtualTable table={table} heightInPx={heightInPx} />
                    )}
                </div>
            </div>
        </div>
    );
}

function formatRate(value: number): string {
    if (value == null) {
        return "-";
    }
    if (value === 0) {
        return "0";
    }
    if (value >= 1) {
        return Math.round(value).toLocaleString();
    }
    return value.toFixed(2);
}

function collectIndexingRows(summary: DebugPackageAnalysisSummary, nodeTag?: string): IndexingRow[] {
    const rows: IndexingRow[] = [];

    Object.entries(summary.SummaryPerNode ?? {}).forEach(([tag, node]) => {
        if (nodeTag && tag !== nodeTag) {
            return;
        }
        const speed = node.DatabaseIndexingSpeed;
        if (speed) {
            rows.push({
                node: tag,
                indexed: speed.IndexedPerSecond,
                mapped: speed.MappedPerSecond,
                reduced: speed.ReducedPerSecond,
            });
        }
    });

    return rows.sort((a, b) => a.node.localeCompare(b.node));
}

import React, { memo, useMemo } from "react";
import Spinner from "react-bootstrap/Spinner";
import { Icon } from "components/common/Icon";
import { useAsync } from "react-async-hook";
import { useServices } from "hooks/useServices";
import { EmptySet } from "components/common/EmptySet";
import { RichAlert } from "components/common/RichAlert";
import {
    ColumnDef,
    getCoreRowModel,
    getFilteredRowModel,
    getSortedRowModel,
    useReactTable,
} from "@tanstack/react-table";
import VirtualTable from "components/common/virtualTable/VirtualTable";
import { virtualTableUtils } from "components/common/virtualTable/utils/virtualTableUtils";
import { analyzerConstants } from "./analyzerConstants";
import SizeGetter from "components/common/SizeGetter";

type IndexStats = Raven.Client.Documents.Indexes.IndexStats;
type IndexState = Raven.Client.Documents.Indexes.IndexState;

const EMPTY_INDEX_STATS: IndexStats[] = [];

interface DatabaseIndexStatsProps {
    packageId: string;
    database: string;
    node: string;
}

interface DatabaseIndexStatsWithSizeProps extends DatabaseIndexStatsProps {
    width: number;
}

function useIndexStatsColumns(availableWidth: number) {
    const bodyWidth = virtualTableUtils.getTableBodyWidth(availableWidth);

    const indexStatsColumns: ColumnDef<IndexStats>[] = useMemo(() => {
        const getSize = virtualTableUtils.getCellSizeProvider(bodyWidth);
        return [
            {
                header: "Index",
                accessorKey: "Name",
                cell: indexNameCell,
                size: getSize(28),
            },
            {
                header: "State",
                accessorKey: "State",
                cell: ({ getValue }) => stateCell(getValue<IndexState>()),
                size: getSize(10),
            },
            {
                header: "Priority",
                accessorKey: "Priority",
                size: getSize(10),
            },
            {
                header: "Type",
                accessorKey: "Type",
                size: getSize(13),
            },
            {
                header: "Entries",
                accessorKey: "EntriesCount",
                cell: ({ getValue }) => formatCount(getValue<number>()),
                size: getSize(9),
            },
            {
                header: "Errors",
                accessorKey: "ErrorsCount",
                cell: indexErrorsCell,
                size: getSize(9),
            },
            {
                header: "Stale",
                id: "stale",
                accessorFn: (index) => index.IsStale,
                cell: indexStaleCell,
                size: getSize(10),
            },
            {
                header: "Lock mode",
                accessorKey: "LockMode",
                size: getSize(11),
            },
        ];
    }, [bodyWidth]);

    return { indexStatsColumns };
}

// On-demand per-node index stats for the selected database, from the analyzer
// databases/indexes/stats endpoint (the summary only has aggregate indexing speed).
export default memo(function DatabaseIndexStats({ packageId, database, node }: DatabaseIndexStatsProps) {
    return (
        <SizeGetter
            render={({ width }) => (
                <DatabaseIndexStatsWithSize packageId={packageId} database={database} node={node} width={width} />
            )}
        />
    );
});

function DatabaseIndexStatsWithSize({ packageId, database, node, width }: DatabaseIndexStatsWithSizeProps) {
    const { manageServerService } = useServices();

    const stats = useAsync(async () => {
        if (!node) {
            return [] as IndexStats[];
        }
        return manageServerService.getDebugPackageDatabaseIndexStats(packageId, node, database);
    }, [packageId, node, database]);

    const indexes = stats.result ?? EMPTY_INDEX_STATS;

    const { indexStatsColumns } = useIndexStatsColumns(width);

    const table = useReactTable({
        data: indexes,
        columns: indexStatsColumns,
        enableSorting: indexes.length > analyzerConstants.minRowsForControls,
        enableColumnFilters: indexes.length > analyzerConstants.minRowsForControls,
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
        getRowId: (row) => row.Name,
    });

    const heightInPx = virtualTableUtils.getHeightInPx(indexes.length, 400);

    return (
        <div className="database-index-stats">
            <div className="panel-bg-1 rounded">
                <div className="p-4 vstack gap-3">
                    <h3 className="m-0">Indexes</h3>
                    {stats.loading ? (
                        <div className="hstack gap-2 justify-content-center text-muted py-3">
                            <Spinner size="sm" /> Loading indexes for node {node}...
                        </div>
                    ) : stats.error ? (
                        <RichAlert variant="danger">
                            Could not load index stats for node {node}. The package may not contain index data for this
                            database, or the report expired.
                        </RichAlert>
                    ) : indexes.length === 0 ? (
                        <EmptySet compact className="justify-content-center">
                            No indexes for {database} on node {node}
                        </EmptySet>
                    ) : (
                        <VirtualTable table={table} heightInPx={heightInPx} />
                    )}
                </div>
            </div>
        </div>
    );
}

function indexNameCell({ getValue }: { getValue: () => unknown }) {
    const name = getValue() as string;
    return (
        <div className="text-truncate fw-bold" title={name}>
            {name}
        </div>
    );
}

function indexErrorsCell({ row }: { row: { original: IndexStats } }) {
    const count = row.original.ErrorsCount ?? 0;
    return <span className={count > 0 ? "text-danger" : ""}>{formatCount(count)}</span>;
}

function indexStaleCell({ getValue }: { getValue: () => unknown }) {
    return getValue() ? (
        <span className="hstack gap-1 text-warning">
            <Icon icon="warning" margin="m-0" /> Stale
        </span>
    ) : (
        "Up to date"
    );
}

function formatCount(value: number): string {
    return value == null ? "-" : value.toLocaleString();
}

function stateCell(state: IndexState) {
    switch (state) {
        case "Error":
            return (
                <span className="hstack gap-1 text-danger">
                    <Icon icon="danger" margin="m-0" /> Error
                </span>
            );
        case "Disabled":
            return (
                <span className="hstack gap-1 text-warning">
                    <Icon icon="cancel" margin="m-0" /> Disabled
                </span>
            );
        case "Idle":
            return (
                <span className="hstack gap-1 text-muted">
                    <Icon icon="clock" margin="m-0" /> Idle
                </span>
            );
        default:
            return (
                <span className="hstack gap-1 text-success">
                    <Icon icon="check" margin="m-0" /> Normal
                </span>
            );
    }
}

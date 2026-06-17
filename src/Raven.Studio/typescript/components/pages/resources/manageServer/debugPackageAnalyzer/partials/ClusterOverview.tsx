import React, { useMemo } from "react";
import {
    ColumnDef,
    getCoreRowModel,
    getFilteredRowModel,
    getSortedRowModel,
    useReactTable,
} from "@tanstack/react-table";
import { EmptySet } from "components/common/EmptySet";
import { Icon } from "components/common/Icon";
import NodeTagPill from "./NodeTagPill";
import StatTile from "./StatTile";
import { formatUpTime, osIcon, parseUpTimeSeconds } from "./analyzerUtils";
import VirtualTable from "components/common/virtualTable/VirtualTable";
import { virtualTableUtils } from "components/common/virtualTable/utils/virtualTableUtils";
import { analyzerConstants } from "./analyzerConstants";
import SizeGetter from "components/common/SizeGetter";

type DebugPackageAnalysisSummary = Raven.Server.Documents.Handlers.Debugging.DebugPackage.DebugPackageAnalysisSummary;
type NodeSummary = DebugPackageAnalysisSummary["SummaryPerNode"][string];
type ClusterNodeInfo = NodeSummary["ClusterNodeInfo"];

interface ClusterOverviewProps {
    summary: DebugPackageAnalysisSummary;
}

interface ClusterOverviewWithSizeProps extends ClusterOverviewProps {
    width: number;
}

function useClusterOverviewColumns(availableWidth: number) {
    const bodyWidth = virtualTableUtils.getTableBodyWidth(
        availableWidth - analyzerConstants.panelHorizontalPaddingInPx
    );
    const getSize = useMemo(() => virtualTableUtils.getCellSizeProvider(bodyWidth), [bodyWidth]);

    const clusterColumns: ColumnDef<ClusterNodeInfo>[] = useMemo(
        () => [
            {
                header: "Node tag",
                accessorKey: "NodeTag",
                cell: ({ getValue }) => <NodeTagPill tag={getValue<string>()} />,
                size: getSize(8),
                enableSorting: true,
            },
            {
                header: "Role",
                accessorKey: "NodeState",
                cell: ClusterRoleCell,
                size: getSize(10),
                enableSorting: true,
            },
            {
                header: "Type",
                accessorKey: "NodeType",
                size: getSize(10),
                enableSorting: true,
            },
            {
                header: "OS",
                accessorKey: "OsName",
                cell: ClusterOsCell,
                size: getSize(18),
                enableSorting: true,
            },
            {
                header: "Server version",
                accessorKey: "ServerVersion",
                size: getSize(14),
                enableSorting: true,
            },
            {
                header: "Uptime",
                id: "uptime",
                accessorFn: (row) => parseUpTimeSeconds(row.UpTime),
                cell: ({ row }) => formatUpTime(row.original.UpTime),
                size: getSize(12),
                enableSorting: true,
            },
            {
                header: "URL",
                accessorKey: "NodeUrl",
                cell: ClusterUrlCell,
                size: getSize(28),
            },
        ],
        [getSize]
    );

    return { clusterColumns };
}

export default function ClusterOverview({ summary }: ClusterOverviewProps) {
    return <SizeGetter render={({ width }) => <ClusterOverviewWithSize summary={summary} width={width} />} />;
}

function ClusterOverviewWithSize({ summary, width }: ClusterOverviewWithSizeProps) {
    const nodes = useMemo(() => Object.values(summary.SummaryPerNode ?? {}) as NodeSummary[], [summary]);
    const nodeInfos = useMemo(() => nodes.map((n) => n.ClusterNodeInfo).filter(Boolean), [nodes]);

    const leader = nodeInfos.find((n) => n.NodeState === "Leader");

    const totalDatabases = useMemo(() => {
        const names = new Set<string>();
        nodes.forEach((n) => (n.DatabasesOverview?.Items ?? []).forEach((item) => names.add(item.Database)));
        return names.size;
    }, [nodes]);

    const nodeCount = nodeInfos.length;

    const clusterUpTime = useMemo(() => {
        let best: string | null = null;
        let bestSeconds = -1;
        nodeInfos.forEach((n) => {
            const seconds = parseUpTimeSeconds(n.UpTime);
            if (seconds > bestSeconds) {
                bestSeconds = seconds;
                best = n.UpTime;
            }
        });
        return best;
    }, [nodeInfos]);

    const { clusterColumns } = useClusterOverviewColumns(width);

    const table = useReactTable({
        data: nodeInfos,
        columns: clusterColumns,
        enableSorting: nodeInfos.length > analyzerConstants.minRowsForControls,
        enableColumnFilters: nodeInfos.length > analyzerConstants.minRowsForControls,
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
        initialState: {
            sorting: [{ id: "NodeTag", desc: false }],
        },
        getRowId: (row) => row.NodeTag,
    });

    const heightInPx = virtualTableUtils.getHeightInPx(nodeInfos.length, 300);

    return (
        <div className="cluster-overview">
            <div className="panel-bg-1 rounded">
                <div className="p-4 vstack">
                    <h3 className="mb-3">Cluster Overview</h3>
                    {nodeInfos.length === 0 ? (
                        <EmptySet compact className="justify-content-center">
                            No cluster overview data in the package
                        </EmptySet>
                    ) : (
                        <>
                            <div className="overview-stats gap-3 mb-3">
                                <StatTile
                                    label="Nodes status"
                                    icon="check"
                                    iconColor="success"
                                    value={`${nodeCount}/${nodeCount} online`}
                                />
                                <StatTile
                                    label="Leader node"
                                    icon="node-leader"
                                    iconColor="node"
                                    value={leader?.NodeTag ?? "-"}
                                />
                                <StatTile
                                    label="Cluster uptime"
                                    icon="clock"
                                    value={clusterUpTime != null ? formatUpTime(clusterUpTime) : "-"}
                                />
                                <StatTile label="Total databases" icon="database" value={String(totalDatabases)} />
                                <StatTile label="License tier" icon="license" value="n/a" />
                            </div>

                            <VirtualTable table={table} heightInPx={heightInPx} />
                        </>
                    )}
                </div>
            </div>
        </div>
    );
}

function ClusterRoleCell({ getValue }: { getValue: () => unknown }) {
    const state = getValue() as string;
    if (!state) {
        return null;
    }
    switch (state) {
        case "Leader":
            return (
                <span className="hstack gap-1">
                    <Icon icon="node-leader" margin="m-0" /> Leader
                </span>
            );
        case "Follower":
            return (
                <span className="hstack gap-1">
                    <Icon icon="cluster-member" margin="m-0" /> Follower
                </span>
            );
        case "Candidate":
            return (
                <span className="hstack gap-1">
                    <Icon icon="question" margin="m-0" /> Candidate
                </span>
            );
        case "Passive":
            return (
                <span className="hstack gap-1">
                    <Icon icon="node" margin="m-0" /> Passive
                </span>
            );
        default:
            return <span>{state}</span>;
    }
}

function ClusterOsCell({ row }: { row: { original: ClusterNodeInfo } }) {
    return (
        <>
            <Icon icon={osIcon(row.original.OsType)} /> {row.original.OsName}
        </>
    );
}

function ClusterUrlCell({ getValue }: { getValue: () => unknown }) {
    const url = getValue() as string;
    return (
        <a href={url} target="_blank" rel="noreferrer">
            {url}
        </a>
    );
}

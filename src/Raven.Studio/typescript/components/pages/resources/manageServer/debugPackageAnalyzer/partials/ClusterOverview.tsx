import React, { useMemo } from "react";
import Card from "react-bootstrap/Card";
import Table from "react-bootstrap/Table";
import { Icon } from "components/common/Icon";
import NodeTagPill from "./NodeTagPill";
import StatTile from "./StatTile";
import { formatUpTime, osIcon, parseUpTimeSeconds } from "./analyzerUtils";
import { SortableHeader, useSortableData } from "./sortableTable";

type DebugPackageAnalysisSummary = Raven.Server.Documents.Handlers.Debugging.DebugPackage.DebugPackageAnalysisSummary;
type ClusterNodeInfo = DebugPackageAnalysisSummary["SummaryPerNode"][string]["ClusterNodeInfo"];

interface ClusterOverviewProps {
    summary: DebugPackageAnalysisSummary;
}

const clusterNodeSortAccessors: Record<string, (node: ClusterNodeInfo) => number | string> = {
    nodeTag: (node) => node.NodeTag,
    state: (node) => node.NodeState,
    type: (node) => node.NodeType,
    os: (node) => node.OsName ?? "",
    version: (node) => node.ServerVersion ?? "",
    uptime: (node) => parseUpTimeSeconds(node.UpTime),
};

export default function ClusterOverview({ summary }: ClusterOverviewProps) {
    const nodes = useMemo(() => Object.values(summary.SummaryPerNode ?? {}), [summary]);
    const nodeInfos = useMemo(() => nodes.map((n) => n.ClusterNodeInfo).filter(Boolean), [nodes]);

    const leader = nodeInfos.find((n) => n.NodeState === "Leader");

    const totalDatabases = useMemo(() => {
        const names = new Set<string>();
        nodes.forEach((n) => (n.DatabasesOverview?.Items ?? []).forEach((item) => names.add(item.Database)));
        return names.size;
    }, [nodes]);

    // a static package has no live online/offline flag - every captured node is treated as online
    const nodeCount = nodeInfos.length;

    // closest proxy for "cluster uptime" available in the summary: the longest-running node
    const clusterUpTime = useMemo(() => {
        let best: string = null;
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

    const { sorted, sortKey, sortDirection, requestSort } = useSortableData(
        nodeInfos,
        clusterNodeSortAccessors,
        "nodeTag",
        "asc"
    );
    const sortProps = { sortKey, sortDirection, onSort: requestSort };

    return (
        <div className="cluster-overview">
            <h3 className="mb-3">Cluster Overview</h3>
            <Card>
                <Card.Body className="vstack gap-3">
                    <div className="overview-stats d-flex gap-3 flex-wrap">
                        <StatTile
                            label="Nodes status"
                            icon="check"
                            iconColor="success"
                            value={`${nodeCount}/${nodeCount} online`}
                        />
                        <StatTile label="Leader node" icon="cluster-member" value={leader?.NodeTag ?? "-"} />
                        <StatTile label="Cluster uptime" icon="clock" value={formatUpTime(clusterUpTime)} />
                        <StatTile label="Total databases" icon="database" value={String(totalDatabases)} />
                        <StatTile label="License tier" icon="license" value="n/a" />
                    </div>

                    <Table responsive className="m-0 align-middle">
                        <thead>
                            <tr>
                                <SortableHeader label="Node tag" columnKey="nodeTag" {...sortProps} />
                                <SortableHeader label="State" columnKey="state" {...sortProps} />
                                <SortableHeader label="Type" columnKey="type" {...sortProps} />
                                <SortableHeader label="OS" columnKey="os" {...sortProps} />
                                <SortableHeader label="Server version" columnKey="version" {...sortProps} />
                                <SortableHeader label="Uptime" columnKey="uptime" {...sortProps} />
                                <th>URL</th>
                            </tr>
                        </thead>
                        <tbody>
                            {sorted.map((node) => (
                                <tr key={node.NodeTag}>
                                    <td>
                                        <NodeTagPill tag={node.NodeTag} />
                                    </td>
                                    <td>{node.NodeState}</td>
                                    <td>{node.NodeType}</td>
                                    <td>
                                        <Icon icon={osIcon(node.OsType)} /> {node.OsName}
                                    </td>
                                    <td>{node.ServerVersion}</td>
                                    <td>{formatUpTime(node.UpTime)}</td>
                                    <td>
                                        <a href={node.NodeUrl} target="_blank" rel="noreferrer">
                                            {node.NodeUrl}
                                        </a>
                                    </td>
                                </tr>
                            ))}
                        </tbody>
                    </Table>
                </Card.Body>
            </Card>
        </div>
    );
}

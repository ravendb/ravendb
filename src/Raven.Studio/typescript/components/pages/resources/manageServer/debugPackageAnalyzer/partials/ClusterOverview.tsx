import React, { useMemo } from "react";
import Card from "react-bootstrap/Card";
import Table from "react-bootstrap/Table";
import { Icon } from "components/common/Icon";
import { StatePill } from "components/common/StatePill";
import IconName from "typings/server/icons";

type DebugPackageAnalysisSummary = Raven.Server.Documents.Handlers.Debugging.DebugPackage.DebugPackageAnalysisSummary;
type OSType = Raven.Client.ServerWide.Operations.OSType;

interface ClusterOverviewProps {
    summary: DebugPackageAnalysisSummary;
}

export default function ClusterOverview({ summary }: ClusterOverviewProps) {
    const nodes = useMemo(() => Object.values(summary.SummaryPerNode ?? {}), [summary]);
    const nodeInfos = useMemo(() => nodes.map((n) => n.ClusterNodeInfo).filter(Boolean), [nodes]);

    const leader = nodeInfos.find((n) => n.NodeState === "Leader");

    const totalDatabases = useMemo(() => {
        const names = new Set<string>();
        nodes.forEach((n) => (n.DatabasesOverview?.Items ?? []).forEach((item) => names.add(item.Database)));
        return names.size;
    }, [nodes]);

    return (
        <div className="cluster-overview">
            <h3 className="mb-3">Cluster Overview</h3>
            <Card>
                <Card.Body className="vstack gap-3">
                    <div className="cluster-overview-stats d-flex gap-3 flex-wrap">
                        <StatTile label="Nodes" icon="cluster" value={String(nodeInfos.length)} />
                        <StatTile label="Leader node" icon="cluster-member" value={leader?.NodeTag ?? "-"} />
                        <StatTile label="Cluster uptime" icon="clock" value={formatUpTime(leader?.UpTime)} />
                        <StatTile label="Total databases" icon="database" value={String(totalDatabases)} />
                    </div>

                    <Table responsive className="m-0 align-middle">
                        <thead>
                            <tr>
                                <th>Node tag</th>
                                <th>State</th>
                                <th>Type</th>
                                <th>OS</th>
                                <th>Server version</th>
                                <th>Uptime</th>
                                <th>URL</th>
                            </tr>
                        </thead>
                        <tbody>
                            {nodeInfos.map((node) => (
                                <tr key={node.NodeTag}>
                                    <td>
                                        <StatePill bg="node">{node.NodeTag}</StatePill>
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

interface StatTileProps {
    label: string;
    icon: IconName;
    value: string;
}

function StatTile({ label, icon, value }: StatTileProps) {
    return (
        <div className="stat-tile well px-3 py-2 rounded">
            <div className="stat-tile-label text-muted small">{label}</div>
            <div className="stat-tile-value hstack gap-1 align-items-center fs-4">
                <Icon icon={icon} margin="m-0" /> {value}
            </div>
        </div>
    );
}

function osIcon(osType: OSType): IconName {
    switch (osType) {
        case "Linux":
            return "linux";
        case "Windows":
            return "windows";
        case "MacOS":
            return "apple";
        default:
            return "server";
    }
}

// UpTime arrives as a serialized .NET TimeSpan (e.g. "45.12:33:00")
function formatUpTime(upTime: string | undefined): string {
    if (!upTime) {
        return "-";
    }

    const match = upTime.match(/^(?:(\d+)\.)?(\d{1,2}):(\d{2}):(\d{2})/);
    if (!match) {
        return upTime;
    }

    const [, daysPart, hoursPart, minutesPart] = match;
    const days = daysPart ? Number(daysPart) : 0;
    const hours = Number(hoursPart);
    const minutes = Number(minutesPart);

    const parts: string[] = [];
    if (days) {
        parts.push(`${days}d`);
    }
    if (days || hours) {
        parts.push(`${hours}h`);
    }
    parts.push(`${minutes}m`);

    return parts.join(" ");
}

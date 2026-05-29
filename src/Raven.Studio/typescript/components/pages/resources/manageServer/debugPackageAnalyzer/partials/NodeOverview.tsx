import React from "react";
import Card from "react-bootstrap/Card";
import StatTile from "./StatTile";
import { StatePill } from "components/common/StatePill";
import { formatUpTime, osIcon } from "./analyzerUtils";

type DebugPackageAnalysisSummary = Raven.Server.Documents.Handlers.Debugging.DebugPackage.DebugPackageAnalysisSummary;

interface NodeOverviewProps {
    summary: DebugPackageAnalysisSummary;
    nodeTag: string;
}

export default function NodeOverview({ summary, nodeTag }: NodeOverviewProps) {
    const node = summary.SummaryPerNode?.[nodeTag]?.ClusterNodeInfo;

    if (!node) {
        return null;
    }

    return (
        <div className="node-overview">
            <h3 className="mb-2 hstack gap-2">
                Node Overview
                <StatePill bg="node">{nodeTag}</StatePill>
            </h3>
            <Card>
                <Card.Body>
                    <div className="overview-stats d-flex gap-3 flex-wrap">
                        <StatTile label="Node status" icon="check" iconColor="success" value="Online" />
                        <StatTile label="State" icon="cluster-member" value={node.NodeState} />
                        <StatTile label="Type" icon="node" value={node.NodeType} />
                        <StatTile label="OS" icon={osIcon(node.OsType)} value={node.OsName} />
                        <StatTile label="Server version" icon="server" value={node.ServerVersion} />
                        <StatTile label="Uptime" icon="clock" value={formatUpTime(node.UpTime)} />
                        <StatTile
                            label="URL"
                            icon="link"
                            value={
                                <a href={node.NodeUrl} target="_blank" rel="noreferrer">
                                    {node.NodeUrl}
                                </a>
                            }
                        />
                    </div>
                </Card.Body>
            </Card>
        </div>
    );
}

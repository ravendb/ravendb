import React from "react";
import StatTile from "./StatTile";
import NodeTagPill from "./NodeTagPill";
import { formatUpTime, osIcon } from "./analyzerUtils";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";

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
            <div className="panel-bg-1 rounded">
                <div className="p-4 vstack gap-3">
                    <h3 className="m-0">Node Overview</h3>
                    <div className="vstack gap-3">
                        <div>
                            <div className="hstack gap-1 align-items-center mb-1">
                                Node <NodeTagPill tag={nodeTag} />
                            </div>
                            <div className="overview-stats gap-2">
                                <StatTile label="Node status" icon="check" iconColor="success" value="Online" />
                                <StatTile
                                    label="State"
                                    icon={node.NodeState === "Leader" ? "node-leader" : "cluster-member"}
                                    iconColor="node"
                                    value={node.NodeState}
                                />
                                <StatTile
                                    label="Type"
                                    icon={
                                        node.NodeType === "Promotable"
                                            ? "cluster-promotable"
                                            : node.NodeType === "Member"
                                              ? "cluster-member"
                                              : "node"
                                    }
                                    iconColor="node"
                                    value={node.NodeType}
                                />
                                <StatTile label="OS" icon={osIcon(node.OsType)} value={node.OsName} />
                                <StatTile label="Server version" icon="server" value={node.ServerVersion} />
                                <StatTile label="Uptime" icon="clock" value={formatUpTime(node.UpTime)} />
                                <StatTile
                                    label="URL"
                                    icon="link"
                                    value={
                                        <PopoverWithHoverWrapper message={node.NodeUrl} placement="top">
                                            <a
                                                href={node.NodeUrl}
                                                target="_blank"
                                                rel="noreferrer"
                                                className="node-url-link"
                                            >
                                                {node.NodeUrl}
                                            </a>
                                        </PopoverWithHoverWrapper>
                                    }
                                />
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    );
}

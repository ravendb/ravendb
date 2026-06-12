import React, { useEffect, useState } from "react";
import Popover from "react-bootstrap/Popover";
import StatTile from "./StatTile";
import NodeTagPill from "./NodeTagPill";
import { formatUpTime, osIcon } from "./analyzerUtils";
import { PopoverWithHover } from "components/common/PopoverWithHover";

type DebugPackageAnalysisSummary = Raven.Server.Documents.Handlers.Debugging.DebugPackage.DebugPackageAnalysisSummary;

interface NodeOverviewProps {
    summary: DebugPackageAnalysisSummary;
    nodeTag: string;
}

export default function NodeOverview({ summary, nodeTag }: NodeOverviewProps) {
    const node = summary.SummaryPerNode?.[nodeTag]?.ClusterNodeInfo;
    const [urlEl, setUrlEl] = useState<HTMLAnchorElement>();
    const [isUrlTruncated, setIsUrlTruncated] = useState(false);

    useEffect(() => {
        if (!urlEl) return;
        const check = () => setIsUrlTruncated(urlEl.scrollWidth > urlEl.offsetWidth);
        const observer = new ResizeObserver(check);
        observer.observe(urlEl);
        return () => observer.disconnect();
    }, [urlEl]);

    if (!node) {
        return null;
    }

    return (
        <div className="node-overview">
            <div className="panel-bg-1 rounded">
                <div className="p-4 vstack gap-3">
                    <div className="vstack gap-1">
                        <h3 className="m-0">Node Overview</h3>
                        <div className="hstack gap-2 text-muted">
                            Node <NodeTagPill tag={nodeTag} />
                        </div>
                    </div>
                    <div className="overview-stats gap-3">
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
                                <>
                                    <a
                                        ref={setUrlEl}
                                        href={node.NodeUrl}
                                        target="_blank"
                                        rel="noreferrer"
                                        className="node-url-link"
                                    >
                                        {node.NodeUrl}
                                    </a>
                                    {isUrlTruncated && (
                                        <PopoverWithHover target={urlEl}>
                                            <Popover.Body>{node.NodeUrl}</Popover.Body>
                                        </PopoverWithHover>
                                    )}
                                </>
                            }
                        />
                    </div>
                </div>
            </div>
        </div>
    );
}

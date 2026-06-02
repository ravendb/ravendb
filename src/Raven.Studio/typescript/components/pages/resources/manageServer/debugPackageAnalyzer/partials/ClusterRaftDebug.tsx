import React, { useMemo, useState } from "react";
import Card from "react-bootstrap/Card";
import Table from "react-bootstrap/Table";
import Spinner from "react-bootstrap/Spinner";
import ProgressBar from "react-bootstrap/ProgressBar";
import { useAsync } from "react-async-hook";
import { useServices } from "hooks/useServices";
import { EmptySet } from "components/common/EmptySet";
import { StatePill } from "components/common/StatePill";
import { Icon } from "components/common/Icon";
import Select, { SelectOption } from "components/common/select/Select";
import genUtils from "common/generalUtils";
import { nodeAwareLoadableData } from "components/models/common";
import {
    ClusterDebugNodeInfo,
    mapRaftDebugView,
} from "components/pages/resources/manageServer/advanced/clusterDebug/partials/common";
import ClusterDebugGlobalInfo from "components/pages/resources/manageServer/advanced/clusterDebug/partials/ClusterDebugGlobalInfo";

type DebugPackageAnalysisSummary = Raven.Server.Documents.Handlers.Debugging.DebugPackage.DebugPackageAnalysisSummary;
type RaftDebugView = Raven.Server.Rachis.RaftDebugView;
type PeerConnection = Raven.Server.Rachis.RaftDebugView.PeerConnection;

interface NodeRaftResult {
    nodeTag: string;
    status: "success" | "failure";
    info?: ClusterDebugNodeInfo;
    view?: RaftDebugView;
}

interface ClusterRaftDebugProps {
    summary: DebugPackageAnalysisSummary;
}

const maxEntriesShown = 500;

// Recreates the live Cluster Debug view from the package's per-node raft log (cluster/log endpoint).
// Reuses the live view's mapRaftDebugView + ClusterDebugGlobalInfo; the summary is adapted for a static
// snapshot (no live-cluster "current node" / server URLs, absolute timestamps instead of now-relative).
export default function ClusterRaftDebug({ summary }: ClusterRaftDebugProps) {
    const { manageServerService } = useServices();
    const packageId = summary.PackageId;
    const nodeTags = useMemo(() => Object.keys(summary.SummaryPerNode ?? {}).sort(), [summary]);

    const raft = useAsync(async () => {
        const settled = await Promise.allSettled(
            nodeTags.map((tag) => manageServerService.getDebugPackageClusterLog(packageId, tag))
        );
        return nodeTags.map((tag, index): NodeRaftResult => {
            const outcome = settled[index];
            if (outcome.status === "fulfilled" && outcome.value) {
                return { nodeTag: tag, status: "success", view: outcome.value, info: mapRaftDebugView(outcome.value) };
            }
            return { nodeTag: tag, status: "failure" };
        });
    }, [packageId, nodeTags]);

    const results = raft.result ?? [];
    const loadableNodes: nodeAwareLoadableData<ClusterDebugNodeInfo>[] = results.map((result) => ({
        nodeTag: result.nodeTag,
        data: result.info,
        status: result.status,
    }));

    return (
        <div className="cluster-raft-debug">
            <h3 className="mb-3">Cluster Debug</h3>
            {raft.loading ? (
                <Card>
                    <Card.Body>
                        <div className="hstack gap-2 justify-content-center text-muted py-3">
                            <Spinner size="sm" /> Loading cluster raft log...
                        </div>
                    </Card.Body>
                </Card>
            ) : results.length === 0 ? (
                <Card>
                    <Card.Body>
                        <EmptySet compact>No cluster raft log in the package</EmptySet>
                    </Card.Body>
                </Card>
            ) : (
                <div className="vstack gap-3">
                    <ClusterDebugGlobalInfo nodes={loadableNodes} />
                    <Card>
                        <Card.Body className="vstack gap-3">
                            <RaftSummaryTable results={results} />
                        </Card.Body>
                    </Card>
                    <LogEntries results={results} />
                </div>
            )}
        </div>
    );
}

function RaftSummaryTable({ results }: { results: NodeRaftResult[] }) {
    return (
        <Table responsive className="m-0 align-middle">
            <thead>
                <tr>
                    <th></th>
                    {results.map((result) => (
                        <th key={result.nodeTag}>
                            <span className="hstack gap-1 align-items-center">
                                <Icon
                                    icon={result.info?.role === "Leader" ? "node-leader" : "cluster-member"}
                                    color="node"
                                    margin="m-0"
                                />
                                Node {result.nodeTag}
                            </span>
                        </th>
                    ))}
                </tr>
            </thead>
            <tbody>
                <SummaryRow label="Role" results={results} render={(info) => info.role} />
                <SummaryRow label="Term" results={results} render={(info) => info.term.toLocaleString()} />
                <tr>
                    <th>Progress</th>
                    {results.map((result) => (
                        <td key={result.nodeTag}>
                            {result.info ? (
                                <ProgressBar
                                    now={result.info.progress}
                                    label={`${result.info.progress}%`}
                                    variant={result.info.progress === 100 ? "success" : "primary"}
                                />
                            ) : (
                                <Unavailable />
                            )}
                        </td>
                    ))}
                </tr>
                <SummaryRow label="Queue size" results={results} render={(info) => info.queueSize.toLocaleString()} />
                <SummaryRow
                    label="Commit index"
                    results={results}
                    render={(info) => info.commitIndex.toLocaleString()}
                />
                <SummaryRow
                    label="Last log entry index"
                    results={results}
                    render={(info) => info.lastLogEntryIndex.toLocaleString()}
                />
                <SummaryRow
                    label="Last committed"
                    results={results}
                    render={(info) =>
                        info.lastCommitedTime ? genUtils.formatUtcDateAsLocal(info.lastCommitedTime) : "n/a"
                    }
                />
                <SummaryRow
                    label="Last appended"
                    results={results}
                    render={(info) =>
                        info.lastAppendedTime ? genUtils.formatUtcDateAsLocal(info.lastAppendedTime) : "n/a"
                    }
                />
                <SummaryRow
                    label="Local version"
                    results={results}
                    render={(info) => info.localVersion.toLocaleString()}
                />
                <tr>
                    <th>Connections</th>
                    {results.map((result) => (
                        <td key={result.nodeTag}>
                            {result.info ? <Connections connections={result.info.connections} /> : <Unavailable />}
                        </td>
                    ))}
                </tr>
                <tr>
                    <th>Critical error</th>
                    {results.map((result) => (
                        <td key={result.nodeTag}>
                            {result.info ? (
                                result.info.criticalError ? (
                                    <StatePill bg="danger">Critical error</StatePill>
                                ) : (
                                    "-"
                                )
                            ) : (
                                <Unavailable />
                            )}
                        </td>
                    ))}
                </tr>
            </tbody>
        </Table>
    );
}

function SummaryRow({
    label,
    results,
    render,
}: {
    label: string;
    results: NodeRaftResult[];
    render: (info: ClusterDebugNodeInfo) => React.ReactNode;
}) {
    return (
        <tr>
            <th>{label}</th>
            {results.map((result) => (
                <td key={result.nodeTag}>{result.info ? render(result.info) : <Unavailable />}</td>
            ))}
        </tr>
    );
}

function Unavailable() {
    return <span className="text-muted">n/a</span>;
}

function Connections({ connections }: { connections: PeerConnection[] }) {
    if (!connections?.length) {
        return <>-</>;
    }
    return (
        <div className="hstack gap-1 flex-wrap">
            {connections.map((connection) => (
                <StatePill key={connection.Destination} bg={connection.Connected ? "success" : "danger"}>
                    <Icon icon={connection.Connected ? "connected" : "disconnected"} margin="m-0" />{" "}
                    {connection.Destination}
                </StatePill>
            ))}
        </div>
    );
}

function LogEntries({ results }: { results: NodeRaftResult[] }) {
    const withLogs = results.filter((result) => result.view);
    const [selectedNode, setSelectedNode] = useState<string>(withLogs[0]?.nodeTag ?? null);

    const selected = withLogs.find((result) => result.nodeTag === selectedNode) ?? withLogs[0];
    const log = selected?.view?.Log;
    const entries = log?.Logs ?? [];
    const shown = entries.slice(-maxEntriesShown);

    const nodeOptions: SelectOption<string>[] = withLogs.map((result) => ({
        value: result.nodeTag,
        label: `Node ${result.nodeTag}`,
    }));

    return (
        <Card>
            <Card.Body className="vstack gap-3">
                <div className="hstack gap-3 align-items-center flex-wrap">
                    <h4 className="m-0">Log entries</h4>
                    {nodeOptions.length > 1 && (
                        <div className="node-select">
                            <Select
                                options={nodeOptions}
                                value={nodeOptions.find((o) => o.value === selected?.nodeTag)}
                                onChange={(option) => option && setSelectedNode(option.value)}
                                isSearchable={false}
                                isRoundedPill
                            />
                        </div>
                    )}
                    {log && (
                        <span className="small-label">
                            {entries.length.toLocaleString()} captured / {log.TotalEntries.toLocaleString()} total
                            {entries.length > maxEntriesShown ? ` (showing last ${maxEntriesShown})` : ""}
                        </span>
                    )}
                </div>

                {entries.length === 0 ? (
                    <EmptySet compact>No log entries captured for this node</EmptySet>
                ) : (
                    <div style={{ maxHeight: "480px", overflow: "auto" }}>
                        <Table responsive className="m-0 align-middle">
                            <thead>
                                <tr>
                                    <th>Index</th>
                                    <th>Term</th>
                                    <th>Command type</th>
                                    <th>Flags</th>
                                    <th>Size</th>
                                    <th>Created at</th>
                                </tr>
                            </thead>
                            <tbody>
                                {shown.map((entry) => (
                                    <tr key={entry.Index}>
                                        <td>{entry.Index.toLocaleString()}</td>
                                        <td>{entry.Term}</td>
                                        <td className="fw-bold">{entry.CommandType}</td>
                                        <td>{entry.Flags}</td>
                                        <td>{genUtils.formatBytesToSize(entry.SizeInBytes)}</td>
                                        <td>{entry.CreateAt ? genUtils.formatUtcDateAsLocal(entry.CreateAt) : "-"}</td>
                                    </tr>
                                ))}
                            </tbody>
                        </Table>
                    </div>
                )}
            </Card.Body>
        </Card>
    );
}

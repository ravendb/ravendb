import React, { useMemo, useState } from "react";
import Spinner from "react-bootstrap/Spinner";
import Table from "react-bootstrap/Table";
import ProgressBar from "react-bootstrap/ProgressBar";
import classNames from "classnames";
import { useAsync } from "react-async-hook";
import { useServices } from "hooks/useServices";
import { EmptySet } from "components/common/EmptySet";
import { StatePill } from "components/common/StatePill";
import { Icon } from "components/common/Icon";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import genUtils from "common/generalUtils";
import moment from "moment";
import {
    ClusterDebugNodeInfo,
    mapRaftDebugView,
} from "components/pages/resources/manageServer/advanced/clusterDebug/partials/common";
import ClusterDebugGlobalInfo from "components/pages/resources/manageServer/advanced/clusterDebug/partials/ClusterDebugGlobalInfo";
import {
    ColumnDef,
    getCoreRowModel,
    getFilteredRowModel,
    getSortedRowModel,
    useReactTable,
} from "@tanstack/react-table";
import VirtualTable from "components/common/virtualTable/VirtualTable";
import { CellWithCopyWrapper } from "components/common/virtualTable/cells/CellWithCopy";
import { virtualTableUtils } from "components/common/virtualTable/utils/virtualTableUtils";
import { analyzerConstants } from "./analyzerConstants";
import SizeGetter from "components/common/SizeGetter";
import "../../advanced/clusterDebug/partials/ClusterDebugSummary.scss";
import "../../advanced/clusterDebug/partials/ClusterDebugEntries.scss";

type DebugPackageAnalysisSummary = Raven.Server.Documents.Handlers.Debugging.DebugPackage.DebugPackageAnalysisSummary;
type RaftDebugView = Raven.Server.Rachis.RaftDebugView;
type PeerConnection = Raven.Server.Rachis.RaftDebugView.PeerConnection;
type LogEntry = Raven.Server.Rachis.RachisConsensus.RachisDebugLogEntry;

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

    const loadableNodes = useMemo(
        () =>
            (raft.result ?? []).map((result) => ({
                nodeTag: result.nodeTag,
                data: result.info,
                status: result.status,
            })),
        [raft.result]
    );
    const results = raft.result ?? [];

    return (
        <div className="cluster-raft-debug">
            {raft.loading ? (
                <div className="hstack gap-2 justify-content-center text-muted py-3">
                    <Spinner size="sm" /> Loading cluster raft log...
                </div>
            ) : results.length === 0 ? (
                <EmptySet compact className="justify-content-center">
                    No cluster raft log in the package
                </EmptySet>
            ) : (
                <div className="vstack gap-3">
                    <div className="panel-bg-1 rounded">
                        <div className="p-4">
                            <h3 className="mb-3">Cluster Debug</h3>
                            <ClusterDebugGlobalInfo nodes={loadableNodes} cardClassName="panel-bg-2" fillWidth />
                            <h4 className="mt-3">Summary</h4>
                            <RaftSummaryTable results={results} />
                        </div>
                        <LogEntriesCard results={results} />
                    </div>
                </div>
            )}
        </div>
    );
}

function RaftSummaryTable({ results }: { results: NodeRaftResult[] }) {
    const hasAnyCriticalError = results.some((r) => !!r.info?.criticalError);

    return (
        <Table bordered responsive className="mb-1 rounded-1 overflow-hidden cluster-debug-summary">
            <thead>
                <tr>
                    <th></th>
                    {results.map((result) => (
                        <th key={result.nodeTag}>
                            <div className="d-flex gap-1 align-items-center">
                                <span>
                                    <Icon
                                        icon={result.info?.role === "Leader" ? "node-leader" : "cluster-member"}
                                        color="node"
                                    />
                                    <span className="text-nowrap">Node {result.nodeTag}</span>
                                </span>
                            </div>
                        </th>
                    ))}
                </tr>
            </thead>
            <tbody>
                <tr>
                    <th>
                        Role / Phase
                        <PopoverWithHoverWrapper
                            message={
                                <>
                                    <ul>
                                        <li>
                                            The node&apos;s role:
                                            <br />(<i>Leader, Follower, Candidate, or Passive</i>).
                                        </li>
                                        <li>
                                            Followers also indicate their current phase:
                                            <br />(<i>Initial, Negotiation, Snapshot, or Steady</i>).
                                        </li>
                                    </ul>
                                </>
                            }
                        >
                            <Icon icon="info" color="info" margin="ms-1" />
                        </PopoverWithHoverWrapper>
                    </th>
                    {results.map((result) => (
                        <td className="align-content-center" key={result.nodeTag}>
                            {result.info ? <>{result.info.role}</> : <Unavailable />}
                        </td>
                    ))}
                </tr>
                <tr>
                    <th>
                        Progress
                        <PopoverWithHoverWrapper message="Percentage of Raft commands committed on the node out of the total in the log.">
                            <Icon icon="info" color="info" margin="ms-1" />
                        </PopoverWithHoverWrapper>
                    </th>
                    {results.map((result) => (
                        <td className="align-content-center" key={result.nodeTag}>
                            {result.info ? (
                                <PopoverWithHoverWrapper
                                    inline={false}
                                    targetStyle={{ width: "100%" }}
                                    message={
                                        <>
                                            First entry index:{" "}
                                            <strong>{result.info.firstEntryIndex.toLocaleString()}</strong>
                                            <br />
                                            Last commit index:{" "}
                                            <strong>{result.info.commitIndex.toLocaleString()}</strong>
                                            <br />
                                            Last log entry index:{" "}
                                            <strong>{result.info.lastLogEntryIndex.toLocaleString()}</strong>
                                        </>
                                    }
                                >
                                    <ProgressBar
                                        variant={result.info.progress === 100 ? "success" : "progress"}
                                        striped={result.info.progress < 100}
                                        now={result.info.progress}
                                        animated={result.info.progress < 100}
                                        label={`${result.info.progress}%`}
                                    />
                                </PopoverWithHoverWrapper>
                            ) : (
                                <Unavailable />
                            )}
                        </td>
                    ))}
                </tr>
                <tr>
                    <th>
                        Queue size
                        <PopoverWithHoverWrapper message="Number of Raft commands left to be committed on the node.">
                            <Icon icon="info" color="info" margin="ms-1" />
                        </PopoverWithHoverWrapper>
                    </th>
                    {results.map((result) => (
                        <td className="align-content-center" key={result.nodeTag}>
                            {result.info ? <>{result.info.queueSize.toLocaleString()}</> : <Unavailable />}
                        </td>
                    ))}
                </tr>
                <tr>
                    <th>
                        Last commit index
                        <PopoverWithHoverWrapper message="The index of the last Raft command that was committed on the node.">
                            <Icon icon="info" color="info" margin="ms-1" />
                        </PopoverWithHoverWrapper>
                    </th>
                    {results.map((result) => (
                        <td
                            key={result.nodeTag}
                            className={classNames(
                                "align-content-center",
                                result.info?.chocked && "bg-faded-warning text-warning"
                            )}
                        >
                            {result.info ? (
                                <>
                                    {result.info.commitIndex.toLocaleString()}
                                    {result.info.chocked && (
                                        <PopoverWithHoverWrapper
                                            message={
                                                <>
                                                    <span className="text-warning">
                                                        <Icon icon="warning" />
                                                        Warning:
                                                    </span>
                                                    <span> No commits for over 2 minutes</span>
                                                </>
                                            }
                                        >
                                            <Icon icon="warning" margin="ms-1" />
                                        </PopoverWithHoverWrapper>
                                    )}
                                </>
                            ) : (
                                <Unavailable />
                            )}
                        </td>
                    ))}
                </tr>
                <tr>
                    <th>
                        Last committed date
                        <PopoverWithHoverWrapper message="The time the last Raft command was committed on the node.">
                            <Icon icon="info" color="info" margin="ms-1" />
                        </PopoverWithHoverWrapper>
                    </th>
                    {results.map((result) => {
                        const asAgo = result.info?.lastCommitedTime
                            ? genUtils.formatDurationByDate(moment.utc(result.info.lastCommitedTime), true)
                            : null;
                        return (
                            <td className="align-content-center" key={result.nodeTag}>
                                {result.info ? (
                                    <PopoverWithHoverWrapper
                                        message={asAgo ? <>{result.info.lastCommitedTime}</> : null}
                                    >
                                        <div>{asAgo ?? "n/a"}</div>
                                    </PopoverWithHoverWrapper>
                                ) : (
                                    <Unavailable />
                                )}
                            </td>
                        );
                    })}
                </tr>
                <tr>
                    <th>
                        Last append date
                        <PopoverWithHoverWrapper message="The time the last command was appended to the Raft log.">
                            <Icon icon="info" color="info" margin="ms-1" />
                        </PopoverWithHoverWrapper>
                    </th>
                    {results.map((result) => {
                        const asAgo = result.info?.lastAppendedTime
                            ? genUtils.formatDurationByDate(moment.utc(result.info.lastAppendedTime), true)
                            : null;
                        return (
                            <td className="align-content-center" key={result.nodeTag}>
                                {result.info ? (
                                    <PopoverWithHoverWrapper message={asAgo ? result.info.lastAppendedTime : null}>
                                        <div>{asAgo ?? "n/a"}</div>
                                    </PopoverWithHoverWrapper>
                                ) : (
                                    <Unavailable />
                                )}
                            </td>
                        );
                    })}
                </tr>
                <tr>
                    <th>
                        Local version
                        <PopoverWithHoverWrapper
                            message={
                                <>
                                    <ul>
                                        <li>
                                            Each Raft command has an ID number associated with it (not the log index).
                                            Newer RavenDB versions may introduce commands with higher version numbers
                                            that are unknown to nodes running older versions.
                                        </li>
                                        <li>
                                            This value shows the highest Raft command version number known to the node.
                                        </li>
                                    </ul>
                                </>
                            }
                        >
                            <Icon icon="info" color="info" margin="ms-1" id="localVersionTooltip" />
                        </PopoverWithHoverWrapper>
                    </th>
                    {results.map((result) => (
                        <td className="align-content-center" key={result.nodeTag}>
                            {result.info ? <>{result.info.localVersion}</> : <Unavailable />}
                        </td>
                    ))}
                </tr>
                <tr>
                    <th>
                        Connection
                        <PopoverWithHoverWrapper message="The node's connection state to other nodes in the cluster.">
                            <Icon icon="info" color="info" margin="ms-1" />
                        </PopoverWithHoverWrapper>
                    </th>
                    {results.map((result) => (
                        <td className="align-content-center" key={result.nodeTag}>
                            {result.info ? <Connections connections={result.info.connections} /> : <Unavailable />}
                        </td>
                    ))}
                </tr>
                {hasAnyCriticalError && (
                    <tr>
                        <th>Cluster Critical Error</th>
                        {results.map((result) => (
                            <td className="align-content-center" key={result.nodeTag}>
                                {result.info ? (
                                    result.info.criticalError ? (
                                        <StatePill bg="danger">Critical error</StatePill>
                                    ) : (
                                        <>-</>
                                    )
                                ) : (
                                    <Unavailable />
                                )}
                            </td>
                        ))}
                    </tr>
                )}
            </tbody>
        </Table>
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

function LogEntriesCard({ results }: { results: NodeRaftResult[] }) {
    const withLogs = useMemo(() => results.filter((r) => r.view), [results]);
    const [selectedNodeTag, setSelectedNodeTag] = useState<string | null>(null);

    const activeNodeTag = selectedNodeTag ?? withLogs[0]?.nodeTag ?? null;
    const selected = withLogs.find((r) => r.nodeTag === activeNodeTag);

    const log = selected?.view?.Log;
    const entries = log?.Logs ?? [];
    const shown = useMemo(() => entries.slice(-maxEntriesShown), [entries]);
    const commitIndex = log?.CommitIndex ?? 0;

    return (
        <div className="p-4 pt-0">
            <h3 className="hstack align-items-center">Log Entries</h3>
            <div className="cluster-debug-entries">
                {withLogs.length === 0 ? (
                    <EmptySet compact className="justify-content-center">
                        No log entries captured for this node
                    </EmptySet>
                ) : (
                    <>
                        <ul className="nav nav-tabs mb-2">
                            {withLogs.map((result) => (
                                <li key={result.nodeTag} className="nav-item">
                                    <button
                                        type="button"
                                        className={classNames("nav-link no-decor", {
                                            active: result.nodeTag === activeNodeTag,
                                        })}
                                        onClick={() => setSelectedNodeTag(result.nodeTag)}
                                    >
                                        <div className="d-flex gap-1 align-items-center">
                                            <Icon
                                                icon={result.info?.role === "Leader" ? "node-leader" : "cluster-member"}
                                                color="node"
                                            />
                                            <span className="text-nowrap">Node {result.nodeTag}</span>
                                        </div>
                                    </button>
                                </li>
                            ))}
                        </ul>
                        {entries.length === 0 ? (
                            <EmptySet compact className="justify-content-center">
                                No log entries captured for this node
                            </EmptySet>
                        ) : (
                            <>
                                {log && (
                                    <div className="px-1 py-2 small text-muted">
                                        {entries.length.toLocaleString()} captured / {log.TotalEntries.toLocaleString()}{" "}
                                        total
                                        {entries.length > maxEntriesShown ? ` (showing last ${maxEntriesShown})` : ""}
                                    </div>
                                )}
                                <SizeGetter
                                    render={({ width }) => (
                                        <LogEntriesTable shown={shown} commitIndex={commitIndex} width={width} />
                                    )}
                                />
                            </>
                        )}
                    </>
                )}
            </div>
        </div>
    );
}

function useLogEntryColumns(availableWidth: number, commitIndex: number) {
    const bodyWidth = virtualTableUtils.getTableBodyWidth(availableWidth);
    const getSize = useMemo(() => virtualTableUtils.getCellSizeProvider(bodyWidth), [bodyWidth]);

    const columns = useMemo<ColumnDef<LogEntry>[]>(
        () => [
            { header: "Index", accessorKey: "Index", cell: CellWithCopyWrapper, size: getSize(10) },
            {
                id: "commandType",
                header: "Command Type",
                accessorKey: "CommandType",
                cell: CellWithCopyWrapper,
                size: getSize(30),
            },
            { header: "Created", accessorKey: "CreateAt", cell: CellWithCopyWrapper, size: getSize(25) },
            {
                header: "Size",
                accessorFn: (row) => genUtils.formatBytesToSize(row.SizeInBytes),
                cell: CellWithCopyWrapper,
                size: getSize(10),
            },
            { header: "Term", accessorKey: "Term", cell: CellWithCopyWrapper, size: getSize(7) },
            {
                header: "Status",
                accessorFn: (row) => (row.Index <= commitIndex ? "Committed" : "Appended"),
                cell: CellWithCopyWrapper,
                size: getSize(18),
            },
        ],
        // eslint-disable-next-line react-hooks/exhaustive-deps
        [getSize, commitIndex]
    );

    return { columns };
}

function LogEntriesTable({ shown, commitIndex, width }: { shown: LogEntry[]; commitIndex: number; width: number }) {
    const { columns } = useLogEntryColumns(width, commitIndex);

    const table = useReactTable({
        data: shown,
        columns,
        enableSorting: shown.length > analyzerConstants.minRowsForControls,
        enableColumnFilters: shown.length > analyzerConstants.minRowsForControls,
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
    });

    return <VirtualTable table={table} heightInPx={virtualTableUtils.getHeightInPx(shown.length, 500)} />;
}

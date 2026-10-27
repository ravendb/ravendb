import React, { useMemo } from "react";
import Badge from "react-bootstrap/Badge";
import Spinner from "react-bootstrap/Spinner";
import Table from "react-bootstrap/Table";
import { useAsync } from "react-async-hook";
import { useServices } from "hooks/useServices";
import { EmptySet } from "components/common/EmptySet";
import {
    deviceClassHelp,
    storageStatusColor,
    storageStatusHelp,
    storageStatusLabel,
    storageTermHelp,
} from "common/storageIoTerms";
import {
    formatBytes,
    formatMs,
    formatPercent,
} from "components/pages/resources/manageServer/advanced/storageIo/storageIoFormat";

type DebugPackageAnalysisSummary = Raven.Server.Documents.Handlers.Debugging.DebugPackage.DebugPackageAnalysisSummary;
type StorageIoAnalysisInfo =
    Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results.StorageIoAnalysisInfo;
type StorageIoDeviceAnalysis =
    Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results.StorageIoDeviceAnalysis;

interface StorageIoPerNodeProps {
    summary: DebugPackageAnalysisSummary;
    nodeTag?: string;
}

interface NodeStorageIo {
    nodeTag: string;
    info: StorageIoAnalysisInfo;
}

// Storage I/O section of the analyzer: is the disk holding a node back, and how the adaptive write path behaved per database
export default function StorageIoPerNode({ summary, nodeTag }: StorageIoPerNodeProps) {
    const { manageServerService } = useServices();
    const packageId = summary.PackageId;
    const nodeTags = useMemo(
        () => (nodeTag ? [nodeTag] : Object.keys(summary.SummaryPerNode ?? {}).sort()),
        [summary, nodeTag]
    );

    const result = useAsync(async (): Promise<NodeStorageIo[]> => {
        const settled = await Promise.allSettled(
            nodeTags.map((tag) => manageServerService.getDebugPackageStorageIo(packageId, tag))
        );
        return nodeTags.map((tag, i) => ({
            nodeTag: tag,
            info:
                settled[i].status === "fulfilled"
                    ? (settled[i] as PromiseFulfilledResult<StorageIoAnalysisInfo>).value
                    : null,
        }));
    }, [packageId, nodeTags]);

    if (result.loading) {
        return <Spinner size="sm" />;
    }

    const nodes = (result.result ?? []).filter((x) => x.info?.Devices?.length > 0);
    if (nodes.length === 0) {
        return (
            <EmptySet compact className="justify-content-center">
                No storage I/O information in this package
            </EmptySet>
        );
    }

    const derived = nodes.some((x) => !x.info.FromStorageIoEndpoint);

    return (
        <div className="vstack gap-3">
            <Table size="sm" responsive className="mb-0">
                <thead>
                    <tr>
                        <th>Node</th>
                        <th>Disk</th>
                        <th>Status</th>
                        <th title={storageTermHelp.drainShare}>Writes paced</th>
                        <th title={storageTermHelp.journalWriteLatency}>Journal writes</th>
                        <th title={storageTermHelp.syncCost}>Data file syncs</th>
                        <th>Disk class</th>
                    </tr>
                </thead>
                <tbody>
                    {nodes.flatMap((node) =>
                        node.info.Devices.map((device) => (
                            <DeviceRow key={`${node.nodeTag}-${device.Name}`} nodeTag={node.nodeTag} device={device} />
                        ))
                    )}
                </tbody>
            </Table>
            {derived && (
                <div className="small text-muted">
                    Nodes without the storage I/O state in the package (older versions): status derived from the
                    io-metrics the package contains, the same way the server decides. Writes are likely paced when data
                    file syncs average more than 100 ms.
                </div>
            )}

            <Table size="sm" responsive className="mb-0">
                <thead>
                    <tr>
                        <th>Node</th>
                        <th>Database</th>
                        <th>Journal compression</th>
                        <th>Journal writes</th>
                        <th title={storageTermHelp.pipelining}>Pipelined</th>
                        <th>Data file syncs</th>
                        <th title="Syncs forced by the unsynced backlog passing the mandatory limit">
                            Mandatory syncs
                        </th>
                        <th title={storageTermHelp.drain}>Paced syncs</th>
                    </tr>
                </thead>
                <tbody>
                    {nodes.flatMap((node) =>
                        node.info.Databases.map((db) => (
                            <tr key={`${node.nodeTag}-${db.Database}`}>
                                <td>
                                    <Badge bg="node" pill>
                                        {node.nodeTag}
                                    </Badge>
                                </td>
                                <td>{db.Database}</td>
                                <td>
                                    {db.CompressionAlgorithms.length > 0 ? (
                                        db.CompressionAlgorithms.join(", ")
                                    ) : (
                                        <NotInPackage />
                                    )}
                                </td>
                                <td>
                                    {formatMs(db.JournalWriteAverageInMs)}{" "}
                                    <span className="text-muted small">({db.JournalWrites.toLocaleString()})</span>
                                </td>
                                <td>
                                    {db.PipelinedShare == null ? <NotInPackage /> : formatPercent(db.PipelinedShare)}
                                </td>
                                <td>
                                    {formatMs(db.SyncAverageInMs)}{" "}
                                    <span className="text-muted small">max {formatMs(db.SyncMaxInMs)}</span>
                                </td>
                                <td className={db.MandatorySyncShare > 0.5 ? "text-warning" : null}>
                                    {db.MandatorySyncShare == null ? (
                                        <NotInPackage />
                                    ) : (
                                        formatPercent(db.MandatorySyncShare)
                                    )}
                                </td>
                                <td>
                                    {db.DrainSyncShare == null ? <NotInPackage /> : formatPercent(db.DrainSyncShare)}
                                </td>
                            </tr>
                        ))
                    )}
                </tbody>
            </Table>

            {nodes.some((x) => x.info.SharedJournals?.length > 0) && (
                <Table size="sm" responsive className="mb-0">
                    <thead>
                        <tr>
                            <th>Node</th>
                            <th>Database</th>
                            <th title={storageTermHelp.sharedJournal}>Indexes sharing the journal</th>
                            <th>Writing their own journals</th>
                            <th>Index commits waiting</th>
                            <th>Commit to durable</th>
                        </tr>
                    </thead>
                    <tbody>
                        {nodes.flatMap((node) =>
                            (node.info.SharedJournals ?? []).map((shared) => {
                                const fellBack = shared.IndexesNotSharing.filter((x) => x.Reason === "HardLinkLimit");
                                return (
                                    <tr key={`${node.nodeTag}-${shared.Database}`}>
                                        <td>
                                            <Badge bg="node" pill>
                                                {node.nodeTag}
                                            </Badge>
                                        </td>
                                        <td>{shared.Database}</td>
                                        <td>{shared.IndexesSharing}</td>
                                        <td className={fellBack.length > 0 ? "text-warning" : null}>
                                            {fellBack.length > 0 ? fellBack.map((x) => x.Index).join(", ") : "none"}
                                        </td>
                                        <td>{shared.MergedCommitsQueue}</td>
                                        <td>
                                            {shared.CommitToDurableInMs > 0
                                                ? formatMs(shared.CommitToDurableInMs)
                                                : "idle"}
                                        </td>
                                    </tr>
                                );
                            })
                        )}
                    </tbody>
                </Table>
            )}
        </div>
    );
}

function DeviceRow({ nodeTag, device }: { nodeTag: string; device: StorageIoDeviceAnalysis }) {
    const status = device.Status ?? (device.IsBottleneck ? "Saturated" : "Ok");

    return (
        <tr>
            <td>
                <Badge bg="node" pill>
                    {nodeTag}
                </Badge>
            </td>
            <td>{device.Name}</td>
            <td>
                <Badge bg={storageStatusColor[status]} title={device.StatusReason ?? storageStatusHelp[status]} pill>
                    {device.Status == null && device.IsBottleneck ? "Bottleneck" : storageStatusLabel[status]}
                </Badge>
            </td>
            <td>
                {device.DrainShareOfLastHour != null
                    ? `${formatPercent(device.DrainShareOfLastHour)} of the last hour`
                    : device.LikelyDraining
                      ? "likely (syncs above the drain threshold)"
                      : "no"}
            </td>
            <td>
                {formatMs(device.JournalWriteLatencyInMs)}{" "}
                <span className="text-muted small">{formatBytes(device.JournalWriteSizeInBytes)}</span>
            </td>
            <td>{formatMs(device.SyncCostInMs)}</td>
            <td title={device.ClassDescription ?? deviceClassHelp[device.Class]}>{device.Class}</td>
        </tr>
    );
}

function NotInPackage() {
    return (
        <span className="text-muted small" title="Not in packages from older versions">
            n/a
        </span>
    );
}

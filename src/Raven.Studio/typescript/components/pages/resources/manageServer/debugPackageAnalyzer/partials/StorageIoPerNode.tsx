import React, { useMemo } from "react";
import Badge from "react-bootstrap/Badge";
import Spinner from "react-bootstrap/Spinner";
import { useAsync } from "react-async-hook";
import { ColumnDef, Row, getCoreRowModel, getExpandedRowModel, useReactTable } from "@tanstack/react-table";
import { useServices } from "hooks/useServices";
import { EmptySet } from "components/common/EmptySet";
import SizeGetter from "components/common/SizeGetter";
import VirtualTable from "components/common/virtualTable/VirtualTable";
import { virtualTableUtils } from "components/common/virtualTable/utils/virtualTableUtils";
import NodeTagPill from "./NodeTagPill";
import { ExpandIndicator, NodeTagPillStack, RangeCell, canExpandNodeRow, expandableRowProps } from "./NodeStackTable";
import { toReplicaRange } from "./analyzerUtils";
import { useExpandAllSync } from "./ExpandAllContext";
import { analyzerConstants } from "./analyzerConstants";
import {
    deviceClassHelp,
    storageStatusColor,
    storageStatusHelp,
    storageStatusLabel,
    storageTermHelp,
    writeModeHelp,
    writeModeLabel,
    writeModeTermHelp,
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
type StorageIoDatabaseAnalysis =
    Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results.StorageIoDatabaseAnalysis;
type SharedJournalsIoInfo = Raven.Server.Storage.SharedJournalsIoInfo;
type StorageDeviceStatus = Raven.Server.Storage.StorageDeviceStatus;

interface StorageIoPerNodeProps {
    summary: DebugPackageAnalysisSummary;
    nodeTag?: string;
}

interface NodeStorageIo {
    nodeTag: string;
    info: StorageIoAnalysisInfo;
}

// A disk or database with the value of each node: a parent row rolls them up, its node rows carry one each
interface GroupedRow<V> {
    key: string;
    name: string;
    nodeTag?: string;
    nodeTags: string[];
    values: V[];
    subRows?: GroupedRow<V>[];
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
        return (
            <Frame>
                <Spinner size="sm" />
            </Frame>
        );
    }

    const nodes = (result.result ?? []).filter((x) => x.info?.Devices?.length > 0);
    if (nodes.length === 0) {
        return (
            <Frame>
                <EmptySet compact className="justify-content-center">
                    No storage I/O information in this package
                </EmptySet>
            </Frame>
        );
    }

    return <SizeGetter render={({ width }) => <StorageIoTables nodes={nodes} width={width} />} />;
}

function StorageIoTables({ nodes, width }: { nodes: NodeStorageIo[]; width: number }) {
    // a share of the width, but never narrower than the values need: on a narrow screen the table scrolls instead of cutting them
    const getSize = useMemo(() => {
        const share = virtualTableUtils.getCellSizeProvider(
            virtualTableUtils.getTableBodyWidth(width - analyzerConstants.panelHorizontalPaddingInPx)
        );
        return (percentage: number, minPx: number) => Math.max(share(percentage), minPx);
    }, [width]);

    const devices = useMemo(
        () => group(nodes.flatMap((n) => n.info.Devices.map((d) => ({ name: d.Name, nodeTag: n.nodeTag, value: d })))),
        [nodes]
    );
    const databases = useMemo(
        () =>
            group(
                nodes.flatMap((n) => n.info.Databases.map((d) => ({ name: d.Database, nodeTag: n.nodeTag, value: d })))
            ),
        [nodes]
    );
    const sharedJournals = useMemo(
        () =>
            group(
                nodes.flatMap((n) =>
                    (n.info.SharedJournals ?? []).map((s) => ({ name: s.Database, nodeTag: n.nodeTag, value: s }))
                )
            ),
        [nodes]
    );

    const deviceColumns = useMemo(() => getDeviceColumns(getSize), [getSize]);
    const databaseColumns = useMemo(() => getDatabaseColumns(getSize), [getSize]);
    const sharedJournalColumns = useMemo(() => getSharedJournalColumns(getSize), [getSize]);

    const derived = nodes.some((x) => !x.info.FromStorageIoEndpoint);

    return (
        <Frame>
            <div className="vstack gap-3">
                <NodeGroupedTable rows={devices} columns={deviceColumns} />
                <div className="small" title={writeModeTermHelp}>
                    Data file writes:{" "}
                    {nodes.map((node) => (
                        <span key={node.nodeTag} className="me-3 text-nowrap">
                            <NodeTagPill tag={node.nodeTag} /> <WriteMode writeMode={node.info.WriteMode} />
                        </span>
                    ))}
                </div>
                {derived && (
                    <div className="small text-muted">
                        Nodes without the storage I/O state in the package (older versions): status derived from the
                        io-metrics the package contains, the same way the server decides. Writes are likely paced when
                        data file syncs average more than 100 ms.
                    </div>
                )}

                {databases.length > 0 && <NodeGroupedTable rows={databases} columns={databaseColumns} />}

                {sharedJournals.length > 0 && <NodeGroupedTable rows={sharedJournals} columns={sharedJournalColumns} />}
            </div>
        </Frame>
    );
}

function NodeGroupedTable<V>({ rows, columns }: { rows: GroupedRow<V>[]; columns: ColumnDef<GroupedRow<V>>[] }) {
    const [expanded, setExpanded] = useExpandAllSync();

    const table = useReactTable({
        data: rows,
        columns,
        state: { expanded },
        onExpandedChange: setExpanded,
        getSubRows: (row) => row.subRows,
        getRowCanExpand: canExpandNodeRow,
        getCoreRowModel: getCoreRowModel(),
        getExpandedRowModel: getExpandedRowModel(),
        getRowId: (row) => row.key,
    });

    return (
        <VirtualTable
            table={table}
            heightInPx={virtualTableUtils.getHeightInPx(table.getRowModel().rows.length, 400)}
            {...expandableRowProps<GroupedRow<V>>()}
        />
    );
}

function group<V>(entries: { name: string; nodeTag: string; value: V }[]): GroupedRow<V>[] {
    const byName = new Map<string, { nodeTag: string; value: V }[]>();
    entries.forEach(({ name, nodeTag, value }) => {
        const members = byName.get(name) ?? [];
        members.push({ nodeTag, value });
        byName.set(name, members);
    });

    return [...byName.keys()]
        .sort((a, b) => a.localeCompare(b))
        .map((name) => {
            const members = byName.get(name).sort((a, b) => a.nodeTag.localeCompare(b.nodeTag));
            return {
                key: name,
                name,
                nodeTag: members.length === 1 ? members[0].nodeTag : undefined,
                nodeTags: members.map((m) => m.nodeTag),
                values: members.map((m) => m.value),
                subRows: members.map((m) => ({
                    key: `${name}/${m.nodeTag}`,
                    name,
                    nodeTag: m.nodeTag,
                    nodeTags: [m.nodeTag],
                    values: [m.value],
                })),
            };
        });
}

function getDeviceColumns(
    getSize: (percentage: number, minPx: number) => number
): ColumnDef<GroupedRow<StorageIoDeviceAnalysis>>[] {
    return [
        { id: "name", header: "Disk", cell: NameCell, size: getSize(8, 90) },
        { id: "node", header: "Node", cell: NodeCell, size: getSize(9, 80) },
        {
            id: "status",
            header: "Status",
            cell: ({ row }) => <DeviceStatus devices={row.original.values} />,
            size: getSize(12, 105),
        },
        {
            id: "paced",
            header: () => <span title={storageTermHelp.drainShare}>Paced</span>,
            cell: ({ row }) => <PacedCell devices={row.original.values} />,
            size: getSize(10, 90),
        },
        {
            id: "journal",
            header: () => (
                <span title="Typical (median) journal write latency of the last hour; the average for older packages">
                    Journal writes
                </span>
            ),
            cell: ({ row }) => (
                <NumberRange
                    values={row.original.values.map(
                        (d) => d.TypicalJournalWriteLatencyInMs ?? d.JournalWriteLatencyInMs
                    )}
                    format={formatMs}
                />
            ),
            size: getSize(12, 125),
        },
        {
            id: "peak",
            header: () => <span title="Peak of the last hour">Peak</span>,
            cell: ({ row }) => (
                <NumberRange values={row.original.values.map((d) => d.PeakJournalWriteLatencyInMs)} format={formatMs} />
            ),
            size: getSize(12, 120),
        },
        {
            id: "slowest",
            header: () => <span title="Slowest single write on the disk in the last hour">Slowest write</span>,
            cell: ({ row }) => <SlowestWrite devices={row.original.values} />,
            size: getSize(13, 135),
        },
        {
            id: "syncs",
            header: () => (
                <span title={`${storageTermHelp.syncCost} none: no sync in the last minute.`}>Data file syncs</span>
            ),
            cell: ({ row }) => (
                <NumberRange
                    values={row.original.values.map((d) => d.SyncCostInMs)}
                    format={(ms) => (ms > 0 ? formatMs(ms) : "none")}
                />
            ),
            size: getSize(13, 130),
        },
        {
            id: "class",
            header: "Disk class",
            cell: ({ row }) => {
                const classes = [...new Set(row.original.values.map((d) => d.Class))];
                const description = row.original.values.length === 1 ? row.original.values[0].ClassDescription : null;
                return (
                    <span title={description ?? classes.map((c) => deviceClassHelp[c]).join(" ")}>
                        {classes.join(", ")}
                    </span>
                );
            },
            size: getSize(11, 100),
        },
    ];
}

function getDatabaseColumns(
    getSize: (percentage: number, minPx: number) => number
): ColumnDef<GroupedRow<StorageIoDatabaseAnalysis>>[] {
    return [
        { id: "name", header: "Database", cell: NameCell, size: getSize(18, 170) },
        { id: "node", header: "Node", cell: NodeCell, size: getSize(9, 80) },
        {
            id: "compression",
            header: () => <span title="Journal compression algorithms seen in the package">Compression</span>,
            cell: ({ row }) => {
                const algorithms = [...new Set(row.original.values.flatMap((d) => d.CompressionAlgorithms))].sort();
                return algorithms.length > 0 ? <>{algorithms.join(", ")}</> : <NotInPackage />;
            },
            size: getSize(11, 115),
        },
        {
            id: "journal",
            header: () => (
                <span title="Average over the writes the package holds, in parentheses their number. io-metrics keeps only the latest writes of each file, so this is a sample, not a total.">
                    Journal writes
                </span>
            ),
            cell: ({ row }) => (
                <>
                    <NumberRange values={row.original.values.map((d) => d.JournalWriteAverageInMs)} format={formatMs} />
                    {row.original.values.length === 1 && (
                        <span className="text-muted small">
                            {" "}
                            ({row.original.values[0].JournalWrites.toLocaleString()})
                        </span>
                    )}
                </>
            ),
            size: getSize(14, 135),
        },
        {
            id: "pipelined",
            header: () => <span title={storageTermHelp.pipelining}>Pipelined</span>,
            cell: ({ row }) => (
                <NumberRange values={row.original.values.map((d) => d.PipelinedShare)} format={formatPercent} />
            ),
            size: getSize(10, 95),
        },
        {
            id: "syncs",
            header: "Data file syncs",
            cell: ({ row }) => (
                <>
                    <NumberRange values={row.original.values.map((d) => d.SyncAverageInMs)} format={formatMs} />
                    {row.original.values.length === 1 && (
                        <span className="text-muted small"> max {formatMs(row.original.values[0].SyncMaxInMs)}</span>
                    )}
                </>
            ),
            size: getSize(16, 150),
        },
        {
            id: "mandatory",
            header: () => (
                <span title="Syncs forced by the unsynced backlog passing the mandatory limit">Mandatory</span>
            ),
            cell: ({ row }) => (
                <NumberRange values={row.original.values.map((d) => d.MandatorySyncShare)} format={formatPercent} />
            ),
            size: getSize(11, 105),
        },
        {
            id: "paced",
            header: () => <span title="Share of the data file syncs in the package that ran paced">Paced</span>,
            cell: ({ row }) => (
                <NumberRange values={row.original.values.map((d) => d.DrainSyncShare)} format={formatPercent} />
            ),
            size: getSize(11, 90),
        },
    ];
}

function getSharedJournalColumns(
    getSize: (percentage: number, minPx: number) => number
): ColumnDef<GroupedRow<SharedJournalsIoInfo>>[] {
    return [
        { id: "name", header: "Database", cell: NameCell, size: getSize(20, 170) },
        { id: "node", header: "Node", cell: NodeCell, size: getSize(10, 80) },
        {
            id: "sharing",
            header: () => <span title={storageTermHelp.sharedJournal}>Sharing</span>,
            cell: ({ row }) => (
                <NumberRange values={row.original.values.map((s) => s.IndexesSharing)} format={String} />
            ),
            size: getSize(12, 90),
        },
        {
            id: "own",
            header: () => (
                <span title="Indexes that left the shared journal (hard link limit) and write their own">
                    Own journals
                </span>
            ),
            cell: ({ row }) => {
                const fellBack = [
                    ...new Set(
                        row.original.values.flatMap((s) =>
                            s.IndexesNotSharing.filter((x) => x.Reason === "HardLinkLimit").map((x) => x.Index)
                        )
                    ),
                ];
                return fellBack.length > 0 ? <span className="text-warning">{fellBack.join(", ")}</span> : <>none</>;
            },
            size: getSize(24, 160),
        },
        {
            id: "queue",
            header: () => <span title="Index commits waiting for the shared journal write">Waiting</span>,
            cell: ({ row }) => (
                <NumberRange values={row.original.values.map((s) => s.MergedCommitsQueue)} format={String} />
            ),
            size: getSize(12, 90),
        },
        {
            id: "durable",
            header: () => (
                <span title="Time from an index commit to its shared journal write being durable">
                    Commit to durable
                </span>
            ),
            cell: ({ row }) => (
                <NumberRange
                    values={row.original.values.map((s) => s.CommitToDurableInMs)}
                    format={(ms) => (ms > 0 ? formatMs(ms) : "idle")}
                />
            ),
            size: getSize(22, 160),
        },
    ];
}

function NameCell<V>({ row }: { row: Row<GroupedRow<V>> }) {
    if (row.depth > 0) {
        return null;
    }
    return (
        <span className="hstack gap-1">
            {row.getCanExpand() && <ExpandIndicator expanded={row.getIsExpanded()} />}
            {row.original.name}
        </span>
    );
}

function NodeCell<V>({ row }: { row: Row<GroupedRow<V>> }) {
    if (row.original.nodeTag) {
        return <NodeTagPill tag={row.original.nodeTag} />;
    }
    return <NodeTagPillStack tags={row.original.nodeTags} />;
}

// the spread across the nodes, or "n/a" when no node has the value (packages from older versions)
function NumberRange({ values, format }: { values: number[]; format: (value: number) => string }) {
    const known = values.filter((x) => x != null);
    if (known.length === 0) {
        return <NotInPackage />;
    }
    return <RangeCell range={toReplicaRange(known)} format={format} />;
}

function statusOf(device: StorageIoDeviceAnalysis): StorageDeviceStatus {
    return device.Status ?? (device.IsBottleneck ? "Saturated" : "Ok");
}

const statusSeverity: Record<StorageDeviceStatus, number> = { Ok: 0, Busy: 1, Saturated: 2 };

// a parent row shows the worst disk of its nodes
function DeviceStatus({ devices }: { devices: StorageIoDeviceAnalysis[] }) {
    const worst = devices.reduce((a, b) => (statusSeverity[statusOf(b)] > statusSeverity[statusOf(a)] ? b : a));
    const status = statusOf(worst);
    return (
        <Badge bg={storageStatusColor[status]} title={worst.StatusReason ?? storageStatusHelp[status]} pill>
            {worst.Status == null && worst.IsBottleneck ? "Bottleneck" : storageStatusLabel[status]}
        </Badge>
    );
}

function PacedCell({ devices }: { devices: StorageIoDeviceAnalysis[] }) {
    if (devices.some((d) => d.DrainShareOfLastHour != null)) {
        return (
            <>
                <NumberRange values={devices.map((d) => d.DrainShareOfLastHour)} format={formatPercent} />
            </>
        );
    }
    return devices.some((d) => d.LikelyDraining) ? (
        <span title="Older package, derived from io-metrics: data file syncs average above the drain threshold">
            likely
        </span>
    ) : (
        <>no</>
    );
}

function SlowestWrite({ devices }: { devices: StorageIoDeviceAnalysis[] }) {
    const slowest = devices
        .map((d) => d.SlowestWrite)
        .filter((x) => x != null)
        .sort((a, b) => b.DurationInSec - a.DurationInSec)[0];
    if (!slowest) {
        return <span className="text-muted">none</span>;
    }
    return (
        <span title={slowest.Path}>
            {formatMs(slowest.DurationInSec * 1000)}{" "}
            <span className="text-muted small">for {formatBytes(slowest.DataWrittenInMb * 1024 * 1024)}</span>
        </span>
    );
}

// same panel and header as the other analyzer sections
function Frame({ children }: { children: React.ReactNode }) {
    return (
        <div className="flex-grow-1">
            <div className="panel-bg-1 rounded">
                <div className="p-4">
                    <h3 className="mb-3">Storage I/O</h3>
                    {children}
                </div>
            </div>
        </div>
    );
}

function WriteMode({ writeMode }: { writeMode: Raven.Server.Storage.StorageWriteModeInfo }) {
    if (!writeMode) {
        return <NotInPackage />;
    }

    const { Effective, Configured } = writeMode;
    const expected = Configured === Effective || (Configured === "Auto" && Effective === "IoRing");
    return (
        <strong title={writeModeHelp[Effective]}>
            {writeModeLabel[Effective]}
            {!expected && <span className="fw-normal text-muted"> (configured {writeModeLabel[Configured]})</span>}
        </strong>
    );
}

function NotInPackage() {
    return (
        <span className="text-muted small" title="Not in packages from older versions">
            n/a
        </span>
    );
}

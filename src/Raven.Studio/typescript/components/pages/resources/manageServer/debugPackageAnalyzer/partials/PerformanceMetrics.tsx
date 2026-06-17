import React, { useMemo, useState } from "react";
import {
    ColumnDef,
    getCoreRowModel,
    getFilteredRowModel,
    getSortedRowModel,
    useReactTable,
} from "@tanstack/react-table";
import { EmptySet } from "components/common/EmptySet";
import Spinner from "react-bootstrap/Spinner";
import { useAsync } from "react-async-hook";
import { useServices } from "hooks/useServices";
import { RichAlert } from "components/common/RichAlert";
import { Icon } from "components/common/Icon";
import StatTile from "./StatTile";
import genUtils from "common/generalUtils";
import { formatNumber, formatPercentage } from "./analyzerUtils";
import Button from "react-bootstrap/Button";
import appUrl from "common/appUrl";
import VirtualTable from "components/common/virtualTable/VirtualTable";
import { virtualTableUtils } from "components/common/virtualTable/utils/virtualTableUtils";
import { analyzerConstants } from "./analyzerConstants";
import classNames from "classnames";
import SizeGetter from "components/common/SizeGetter";
import SegmentedControl from "components/common/SegmentedControl";
import IconName from "typings/server/icons";

type DebugPackageAnalysisSummary = Raven.Server.Documents.Handlers.Debugging.DebugPackage.DebugPackageAnalysisSummary;
type CpuUsageAnalysisInfo =
    Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results.CpuUsageAnalysisInfo;
type MemoryAnalysisInfo =
    Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results.Memory.MemoryAnalysisInfo;
type GcMemoryInfo = Raven.Server.Dashboard.Cluster.Notifications.GcInfoPayload.GcMemoryInfo;
type GenerationInfoSize = Raven.Server.Dashboard.Cluster.Notifications.GcInfoPayload.GenerationInfoSize;
type TcpConnections = Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results.TcpConnections;
type PingResult = Raven.Server.Documents.Handlers.Debugging.NodeDebugHandler.PingResult;
type ThreadInfo = Raven.Server.Dashboard.ThreadInfo;

type MetricTab = "cpu" | "memory" | "gc" | "network" | "threads";

interface PerformanceMetricsProps {
    summary: DebugPackageAnalysisSummary;
    nodeTag: string;
}

interface PerformanceMetricsWithSizeProps extends PerformanceMetricsProps {
    width: number;
}

const metricTabs: { label: string; value: MetricTab; icon: IconName }[] = [
    { label: "CPU", value: "cpu", icon: "processor" },
    { label: "Memory", value: "memory", icon: "memory" },
    { label: "GC", value: "gc", icon: "gc" },
    { label: "Network", value: "network", icon: "global" },
    { label: "Threads", value: "threads", icon: "thread-stack-trace" },
];

export default function PerformanceMetrics({ summary, nodeTag }: PerformanceMetricsProps) {
    return (
        <SizeGetter
            render={({ width }) => <PerformanceMetricsWithSize summary={summary} nodeTag={nodeTag} width={width} />}
        />
    );
}

function PerformanceMetricsWithSize({ summary, nodeTag, width }: PerformanceMetricsWithSizeProps) {
    const node = summary.SummaryPerNode?.[nodeTag];
    const [tab, setTab] = useState<MetricTab>("cpu");

    return (
        <div className="performance-metrics">
            <div className="panel-bg-1 rounded">
                <div className="p-4 vstack gap-3">
                    <h3 className="m-0">Performance Metrics</h3>
                    <SegmentedControl<MetricTab> items={metricTabs} selected={tab} onSelect={setTab} fullWidth />
                    {tab === "cpu" && <CpuTab cpu={node?.CpuUsageInfo} width={width} />}
                    {tab === "memory" && <MemoryTab memory={node?.MemoryUsageInfo} width={width} />}
                    {tab === "gc" && <GcTab gc={node?.GcInfo} width={width} />}
                    {tab === "network" && <NetworkTab packageId={summary.PackageId} nodeTag={nodeTag} width={width} />}
                    {tab === "threads" && (
                        <>
                            <div className="hstack">
                                <Button
                                    variant="secondary"
                                    size="sm"
                                    className="ms-auto"
                                    href={appUrl.forCaptureStackTraces(summary.PackageId, nodeTag)}
                                    target="_blank"
                                    rel="noreferrer"
                                >
                                    <Icon icon="stack-traces" /> Open in Stack Traces viewer
                                </Button>
                            </div>
                            <ThreadsTab packageId={summary.PackageId} nodeTag={nodeTag} width={width} />
                        </>
                    )}
                </div>
            </div>
        </div>
    );
}

// --- CPU tab ---

function CpuTab({ cpu, width }: { cpu?: CpuUsageAnalysisInfo; width: number }) {
    if (!cpu) {
        return (
            <EmptySet compact className="justify-content-center">
                No CPU data in the package
            </EmptySet>
        );
    }
    return (
        <>
            <div className="overview-stats gap-3">
                <StatTile label="Process CPU" icon="hammer-driver" value={formatPercentage(cpu.CurrentCpuUsage)} />
                <StatTile
                    label="Machine CPU"
                    icon="studio-configuration"
                    value={formatPercentage(cpu.CurrentMachineCpuUsage)}
                />
                <StatTile label="Average CPU" icon="graph-range" value={formatPercentage(cpu.AverageCpuUsage)} />
                <StatTile label="Kernel time" icon="settings" value={formatPercentage(cpu.KernelTimePercentage)} />
                <StatTile label="Cores" icon="processor" value={formatNumber(cpu.NumberOfCores)} />
                <StatTile label="Utilized cores" icon="swap" value={formatNumber(cpu.UtilizedCores)} />
            </div>
            <div className="d-flex gap-3">
                <ThreadList
                    className="flex-fill"
                    title="Top current CPU usage threads"
                    threads={cpu.TopCurrentCpuUsageThreads}
                    width={Math.floor(width / 2)}
                />
                <ThreadList
                    className="flex-fill"
                    title="Top overall CPU usage threads"
                    threads={cpu.TopOverallCpuUsageThreads}
                    width={Math.floor(width / 2)}
                />
            </div>
        </>
    );
}

type ThreadItem = { name: string };

function useThreadListColumns(availableWidth: number) {
    const bodyWidth = virtualTableUtils.getTableBodyWidth(
        availableWidth - analyzerConstants.panelHorizontalPaddingInPx
    );
    const getSize = virtualTableUtils.getCellSizeProvider(bodyWidth);

    const threadListColumns: ColumnDef<ThreadItem>[] = useMemo(
        () => [
            {
                header: "Thread",
                accessorKey: "name",
                size: getSize(100),
            },
        ],
        [getSize]
    );

    return { threadListColumns };
}

function ThreadList({
    title,
    threads,
    className,
    width,
}: {
    title: string;
    threads: string[];
    className?: string;
    width: number;
}) {
    const data = useMemo<ThreadItem[]>(() => (threads ?? []).map((name) => ({ name })), [threads]);
    const { threadListColumns } = useThreadListColumns(width);

    const table = useReactTable({
        data,
        columns: threadListColumns,
        enableSorting: data.length > analyzerConstants.minRowsForControls,
        enableColumnFilters: data.length > analyzerConstants.minRowsForControls,
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
        getRowId: (_, index) => String(index),
    });

    if (!threads || threads.length === 0) {
        return null;
    }

    return (
        <div className={classNames("overflow-hidden", className)}>
            <div className="small-label ms-1 mb-1">{title}</div>
            <VirtualTable table={table} heightInPx={virtualTableUtils.getHeightInPx(data.length, 300)} />
        </div>
    );
}

// --- Memory tab ---

type MemoryMetric = { label: string; value: string };

function useMemoryMetricColumns(availableWidth: number) {
    const bodyWidth = virtualTableUtils.getTableBodyWidth(
        availableWidth - analyzerConstants.panelHorizontalPaddingInPx
    );
    const getSize = virtualTableUtils.getCellSizeProvider(bodyWidth);

    const memoryMetricColumns: ColumnDef<MemoryMetric>[] = useMemo(
        () => [
            {
                header: "Metric",
                accessorKey: "label",
                size: getSize(75),
            },
            {
                header: "Value",
                accessorKey: "value",
                cell: MemoryValueCell,
                size: getSize(25),
            },
        ],
        [getSize]
    );

    return { memoryMetricColumns };
}

function MemoryValueCell({ getValue }: { getValue: () => unknown }) {
    return <span>{(getValue() as string) ?? "-"}</span>;
}

function MemoryTab({ memory, width }: { memory?: MemoryAnalysisInfo; width: number }) {
    const metrics = useMemo<MemoryMetric[]>(() => {
        if (!memory) {
            return [];
        }
        return [
            { label: "Managed allocations", value: memory.Managed?.ManagedAllocations },
            {
                label: "Lucene managed allocations (term cache)",
                value: memory.Managed?.LuceneManagedAllocationsForTermCache,
            },
            { label: "Unmanaged allocations", value: memory.Unmanaged?.UnmanagedAllocations },
            { label: "Encryption buffers in use", value: memory.Unmanaged?.EncryptionBuffersInUse },
            { label: "Encryption buffers pool", value: memory.Unmanaged?.EncryptionBuffersPool },
            { label: "Encryption locked memory", value: memory.Unmanaged?.EncryptionLockedMemory },
            {
                label: "Lucene unmanaged allocations (sorting)",
                value: memory.Unmanaged?.LuceneUnmanagedAllocationsForSorting,
            },
            {
                label: "Lucene unmanaged allocations (term cache)",
                value: memory.Unmanaged?.LuceneUnmanagedAllocationsForTermCache,
            },
        ];
    }, [memory]);

    const { memoryMetricColumns } = useMemoryMetricColumns(width);

    const table = useReactTable({
        data: metrics,
        columns: memoryMetricColumns,
        enableSorting: metrics.length > analyzerConstants.minRowsForControls,
        enableColumnFilters: metrics.length > analyzerConstants.minRowsForControls,
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
        getRowId: (_, index) => String(index),
    });

    const heightInPx = virtualTableUtils.getHeightInPx(metrics.length, 400);

    if (!memory) {
        return (
            <EmptySet compact className="justify-content-center">
                No memory data in the package
            </EmptySet>
        );
    }

    const warnColor = memory.IsHighDirty ? "warning" : undefined;

    return (
        <>
            <div className="overview-stats gap-3">
                <StatTile label="Working set" icon="storage-used" value={memory.WorkingSet} />
                <StatTile label="Physical memory" icon="memory" value={memory.PhysicalMemory} />
                <StatTile label="Available memory" icon="storage-free" value={memory.AvailableMemory} />
                <StatTile label="Available for processing" icon="swap" value={memory.AvailableMemoryForProcessing} />
                <StatTile
                    label="Dirty memory"
                    icon="clean"
                    iconColor={warnColor}
                    value={memory.DirtyMemory}
                    valueColor={warnColor}
                />
                <StatTile label="Memory mapped" icon="map" value={memory.MemoryMapped} />
            </div>
            <VirtualTable table={table} heightInPx={heightInPx} />
        </>
    );
}

// --- GC tab ---

type GenerationRow = { label: string; size: GenerationInfoSize };

function useGcGenerationColumns(availableWidth: number) {
    const bodyWidth = virtualTableUtils.getTableBodyWidth(
        availableWidth - analyzerConstants.panelHorizontalPaddingInPx
    );
    const getSize = virtualTableUtils.getCellSizeProvider(bodyWidth);

    const gcGenerationColumns: ColumnDef<GenerationRow>[] = useMemo(
        () => [
            { header: "Generation", accessorKey: "label", size: getSize(22) },
            {
                header: "Size before",
                id: "sizeBefore",
                accessorFn: (row) => row.size?.SizeBeforeBytes ?? 0,
                cell: ({ getValue }) => genUtils.formatBytesToSize(getValue<number>()),
                size: getSize(18),
            },
            {
                header: "Size after",
                id: "sizeAfter",
                accessorFn: (row) => row.size?.SizeAfterBytes ?? 0,
                cell: ({ getValue }) => genUtils.formatBytesToSize(getValue<number>()),
                size: getSize(18),
            },
            {
                header: "Fragmentation before",
                id: "fragBefore",
                accessorFn: (row) => row.size?.FragmentationBeforeBytes ?? 0,
                cell: ({ getValue }) => genUtils.formatBytesToSize(getValue<number>()),
                size: getSize(21),
            },
            {
                header: "Fragmentation after",
                id: "fragAfter",
                accessorFn: (row) => row.size?.FragmentationAfterBytes ?? 0,
                cell: ({ getValue }) => genUtils.formatBytesToSize(getValue<number>()),
                size: getSize(21),
            },
        ],
        [getSize]
    );

    return { gcGenerationColumns };
}

function GcTab({ gc, width }: { gc?: GcMemoryInfo; width: number }) {
    const generations = useMemo<GenerationRow[]>(() => {
        if (!gc) {
            return [];
        }
        return [
            { label: "Gen 0", size: gc.Gen0HeapSize },
            { label: "Gen 1", size: gc.Gen1HeapSize },
            { label: "Gen 2", size: gc.Gen2HeapSize },
            { label: "Large object heap", size: gc.LargeObjectHeapSize },
            { label: "Pinned object heap", size: gc.PinnedObjectHeapSize },
        ];
    }, [gc]);

    const { gcGenerationColumns } = useGcGenerationColumns(width);

    const table = useReactTable({
        data: generations,
        columns: gcGenerationColumns,
        enableSorting: generations.length > analyzerConstants.minRowsForControls,
        enableColumnFilters: generations.length > analyzerConstants.minRowsForControls,
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
        getRowId: (_, index) => String(index),
    });

    const heightInPx = virtualTableUtils.getHeightInPx(generations.length, 400);

    if (!gc) {
        return (
            <EmptySet compact className="justify-content-center">
                No GC data in the package
            </EmptySet>
        );
    }

    return (
        <>
            <div className="overview-stats gap-3">
                <StatTile label="Last GC generation" icon="gc" value={`Gen ${gc.Generation}`} />
                <StatTile label="GC index" icon="hash" value={formatNumber(gc.Index)} />
                <StatTile label="Pause time" icon="pause" value={formatPercentage(gc.PauseTimePercentage)} />
                <StatTile
                    label="Total heap after"
                    icon="memory"
                    value={genUtils.formatBytesToSize(gc.TotalHeapSizeAfterBytes)}
                />
                <StatTile label="Concurrent" icon="shuffle" value={gc.Concurrent ? "Yes" : "No"} />
                <StatTile
                    label="Compacted"
                    icon="compact"
                    value={gc.Compacted ? "Yes" : "No"}
                    valueColor={gc.Compacted ? "success" : undefined}
                />
            </div>
            <div>
                <div className="small-label ms-1 mb-1">Heap by generation</div>
                <VirtualTable table={table} heightInPx={heightInPx} />
            </div>
            {gc.PauseDurationsInMs?.length > 0 && (
                <div className="text-muted">
                    Pause durations: {gc.PauseDurationsInMs.map((ms) => `${ms} ms`).join(", ")}
                </div>
            )}
        </>
    );
}

// --- Network tab ---

function useNetworkColumns(availableWidth: number) {
    const bodyWidth = virtualTableUtils.getTableBodyWidth(
        availableWidth - analyzerConstants.panelHorizontalPaddingInPx
    );
    const getSize = virtualTableUtils.getCellSizeProvider(bodyWidth);

    const tcpColumns: ColumnDef<TcpConnections>[] = useMemo(
        () => [
            {
                header: "TCP state",
                id: "state",
                accessorFn: (row) => row.TcpState ?? "",
                size: getSize(25),
                enableSorting: true,
            },
            {
                header: "Connections",
                id: "connections",
                accessorFn: (row) => row.NumberOfConnectionsInState ?? 0,
                cell: ({ getValue }) => formatNumber(getValue<number>()),
                size: getSize(18),
                enableSorting: true,
            },
            {
                header: "Top remote endpoints",
                id: "endpoints",
                accessorFn: (row) => formatTopConnections(row.TopConnectionsInState),
                size: getSize(57),
            },
        ],
        [getSize]
    );

    const pingColumns: ColumnDef<PingResult>[] = useMemo(
        () => [
            {
                header: "Target node",
                accessorKey: "Url",
                size: getSize(35),
                enableSorting: true,
            },
            {
                header: "Setup-alive",
                id: "setup",
                accessorFn: (row) => row.SetupAlive?.Time ?? 0,
                cell: ({ row }) => formatPingMs(row.original.SetupAlive?.Time),
                size: getSize(18),
                enableSorting: true,
            },
            {
                header: "TCP ping",
                id: "tcp",
                accessorFn: (row) => row.TcpInfo?.ReceiveTime ?? 0,
                cell: PingTcpCell,
                size: getSize(18),
                enableSorting: true,
            },
            {
                header: "Status",
                id: "status",
                accessorFn: (row) => (row.SetupAlive?.Error || row.TcpInfo?.Error ? 1 : 0),
                cell: PingStatusCell,
                size: getSize(29),
                enableSorting: true,
            },
        ],
        [getSize]
    );

    return { tcpColumns, pingColumns };
}

function PingTcpCell({ row }: { row: { original: PingResult } }) {
    const tcpPing = row.original.TcpInfo?.ReceiveTime;
    const pingClass = tcpPing > 5000 ? "text-danger" : tcpPing > 2000 ? "text-warning" : "";
    return <span className={pingClass}>{formatPingMs(tcpPing)}</span>;
}

function PingStatusCell({ row }: { row: { original: PingResult } }) {
    const setupError = row.original.SetupAlive?.Error;
    const tcpError = row.original.TcpInfo?.Error;
    const hasError = Boolean(setupError || tcpError);
    return hasError ? (
        <span className="text-danger" title={[setupError, tcpError].filter(Boolean).join(" | ")}>
            <Icon icon="warning" margin="m-0" /> Error
        </span>
    ) : (
        <span className="hstack gap-1 text-success">
            <Icon icon="check" margin="m-0" /> OK
        </span>
    );
}

// Network info is not in the summary; fetch it on demand from the analyzer network endpoint.
function NetworkTab({ packageId, nodeTag, width }: { packageId: string; nodeTag: string; width: number }) {
    const { manageServerService } = useServices();
    const network = useAsync(
        () => manageServerService.getDebugPackageNetworkInfo(packageId, nodeTag),
        [packageId, nodeTag]
    );

    const { tcpColumns, pingColumns } = useNetworkColumns(width);

    const tcpTable = useReactTable({
        data: network.result?.TcpConnections ?? [],
        columns: tcpColumns,
        enableSorting: (network.result?.TcpConnections ?? []).length > analyzerConstants.minRowsForControls,
        enableColumnFilters: (network.result?.TcpConnections ?? []).length > analyzerConstants.minRowsForControls,
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
        initialState: { sorting: [{ id: "connections", desc: true }] },
        getRowId: (_, index) => String(index),
    });

    const pingTable = useReactTable({
        data: network.result?.PingTestResults ?? [],
        columns: pingColumns,
        enableSorting: (network.result?.PingTestResults ?? []).length > analyzerConstants.minRowsForControls,
        enableColumnFilters: (network.result?.PingTestResults ?? []).length > analyzerConstants.minRowsForControls,
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
        initialState: { sorting: [{ id: "tcp", desc: false }] },
        getRowId: (row) => row.Url ?? "",
    });

    const tcpCount = network.result?.TcpConnections?.length ?? 0;
    const pingCount = network.result?.PingTestResults?.length ?? 0;
    const tcpHeightInPx = virtualTableUtils.getHeightInPx(tcpCount, 300);
    const pingHeightInPx = virtualTableUtils.getHeightInPx(pingCount, 300);

    if (network.loading) {
        return (
            <div className="hstack gap-2 justify-content-center text-muted py-3">
                <Spinner size="sm" /> Loading network info for node {nodeTag}...
            </div>
        );
    }

    if (network.error) {
        return (
            <RichAlert variant="warning">
                Could not load network info for node {nodeTag}. The analysis report may have expired on the server
                (re-upload the package to inspect), or this data was not captured.
            </RichAlert>
        );
    }

    const info = network.result;
    if (!info) {
        return (
            <EmptySet compact className="justify-content-center">
                No network data in the package
            </EmptySet>
        );
    }

    return (
        <>
            <div className="overview-stats gap-3">
                <StatTile
                    label="Active TCP connections"
                    icon="global"
                    value={formatNumber(info.TotalActiveTcpConnections)}
                />
                <StatTile label="Connection states" icon="link" value={formatNumber(info.TcpConnections?.length)} />
            </div>
            {tcpCount === 0 ? (
                <EmptySet compact className="justify-content-center">
                    No TCP connection data in the package
                </EmptySet>
            ) : (
                <VirtualTable table={tcpTable} heightInPx={tcpHeightInPx} />
            )}
            <div>
                <div className="small-label ms-1 mb-1">Node-to-node ping (from node {nodeTag})</div>
                {pingCount === 0 ? (
                    <EmptySet compact className="justify-content-center">
                        No ping test data in the package
                    </EmptySet>
                ) : (
                    <VirtualTable table={pingTable} heightInPx={pingHeightInPx} />
                )}
            </div>
        </>
    );
}

// --- Threads tab ---

function useThreadColumns(availableWidth: number) {
    const bodyWidth = virtualTableUtils.getTableBodyWidth(
        availableWidth - analyzerConstants.panelHorizontalPaddingInPx
    );
    const getSize = virtualTableUtils.getCellSizeProvider(bodyWidth);

    const threadColumns: ColumnDef<ThreadInfo>[] = useMemo(
        () => [
            {
                header: "Thread",
                id: "name",
                accessorFn: (row) => row.Name ?? "",
                cell: ThreadNameCell,
                size: getSize(22),
                enableSorting: true,
            },
            {
                header: "CPU",
                id: "cpu",
                accessorFn: (row) => row.CpuUsage ?? 0,
                cell: ({ row }) => formatPercentage(row.original.CpuUsage),
                size: getSize(10),
                enableSorting: true,
            },
            {
                header: "State",
                id: "state",
                accessorFn: (row) => row.State ?? "",
                size: getSize(13),
                enableSorting: true,
            },
            {
                header: "Processor time",
                id: "processorTime",
                accessorFn: (row) => genUtils.timeSpanToSeconds(row.TotalProcessorTime) ?? 0,
                cell: ({ row }) => row.original.TotalProcessorTime,
                size: getSize(16),
                enableSorting: true,
            },
            {
                header: "Unmanaged alloc.",
                id: "unmanaged",
                accessorFn: (row) => row.UnmanagedAllocationsInBytes ?? 0,
                cell: ({ row }) => genUtils.formatBytesToSize(row.original.UnmanagedAllocationsInBytes ?? 0),
                size: getSize(16),
                enableSorting: true,
            },
            {
                header: "IO read",
                id: "ioRead",
                accessorFn: (row) => row.IoStats?.ReadBytes ?? 0,
                cell: ({ row }) => formatThreadIo(row.original.IoStats?.ReadBytes),
                size: getSize(12),
                enableSorting: true,
            },
            {
                header: "IO write",
                id: "ioWrite",
                accessorFn: (row) => row.IoStats?.WriteBytes ?? 0,
                cell: ({ row }) => formatThreadIo(row.original.IoStats?.WriteBytes),
                size: getSize(11),
                enableSorting: true,
            },
        ],
        [getSize]
    );

    return { threadColumns };
}

function ThreadNameCell({ row }: { row: { original: ThreadInfo } }) {
    return (
        <>
            <div className="text-break">{row.original.Name}</div>
            <div className="small-label">#{row.original.Id}</div>
        </>
    );
}

// Thread runtime info is not in the summary; fetch on demand from the analyzer threads/runaway endpoint.
function ThreadsTab({ packageId, nodeTag, width }: { packageId: string; nodeTag: string; width: number }) {
    const { manageServerService } = useServices();
    const threads = useAsync(
        () => manageServerService.getDebugPackageThreadsInfo(packageId, nodeTag),
        [packageId, nodeTag]
    );

    const { threadColumns } = useThreadColumns(width);

    const table = useReactTable({
        data: threads.result?.List ?? [],
        columns: threadColumns,
        enableSorting: (threads.result?.List ?? []).length > analyzerConstants.minRowsForControls,
        enableColumnFilters: (threads.result?.List ?? []).length > analyzerConstants.minRowsForControls,
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
        initialState: { sorting: [{ id: "cpu", desc: true }] },
        getRowId: (row) => String(row.Id),
    });

    const heightInPx = virtualTableUtils.getHeightInPx(threads.result?.List?.length ?? 0, 500);

    if (threads.loading) {
        return (
            <div className="hstack gap-2 justify-content-center text-muted py-3">
                <Spinner size="sm" /> Loading threads for node {nodeTag}...
            </div>
        );
    }

    if (threads.error) {
        return (
            <RichAlert variant="warning">
                Could not load threads for node {nodeTag}. The analysis report may have expired on the server (re-upload
                the package to inspect), or this data was not captured.
            </RichAlert>
        );
    }

    const info = threads.result;
    if (!info) {
        return (
            <EmptySet compact className="justify-content-center">
                No threads data in the package
            </EmptySet>
        );
    }

    return (
        <>
            <div className="overview-stats gap-3">
                <StatTile label="Process CPU" icon="hammer-driver" value={formatPercentage(info.ProcessCpuUsage)} />
                <StatTile label="Threads" icon="thread-stack-trace" value={formatNumber(info.ThreadsCount)} />
                <StatTile
                    label="Dedicated threads"
                    icon="stack-traces"
                    value={formatNumber(info.DedicatedThreadsCount)}
                />
                <StatTile label="Active cores" icon="processor" value={formatNumber(info.ActiveCores)} />
            </div>
            {table.getRowCount() === 0 ? (
                <EmptySet compact className="justify-content-center">
                    No threads in the package
                </EmptySet>
            ) : (
                <VirtualTable table={table} heightInPx={heightInPx} />
            )}
        </>
    );
}

function formatPingMs(ms: number | undefined): string {
    return ms == null ? "-" : `${formatNumber(ms)} ms`;
}

function formatTopConnections(top: { [endpoint: string]: number }): string {
    const entries = Object.entries(top ?? {});
    if (entries.length === 0) {
        return "-";
    }
    return entries
        .sort((a, b) => b[1] - a[1])
        .map(([endpoint, count]) => `${endpoint} (${count})`)
        .join(", ");
}

function formatThreadIo(bytes: number | undefined): string {
    return bytes == null ? "-" : genUtils.formatBytesToSize(bytes);
}

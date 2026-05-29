import React, { useState } from "react";
import Card from "react-bootstrap/Card";
import Table from "react-bootstrap/Table";
import { MultiRadioToggle } from "components/common/toggles/MultiRadioToggle";
import { InputItem } from "components/models/common";
import { EmptySet } from "components/common/EmptySet";
import StatTile from "./StatTile";
import genUtils from "common/generalUtils";

type DebugPackageAnalysisSummary = Raven.Server.Documents.Handlers.Debugging.DebugPackage.DebugPackageAnalysisSummary;
type CpuUsageAnalysisInfo =
    Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results.CpuUsageAnalysisInfo;
type MemoryAnalysisInfo =
    Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results.Memory.MemoryAnalysisInfo;
type GcMemoryInfo = Raven.Server.Dashboard.Cluster.Notifications.GcInfoPayload.GcMemoryInfo;
type GenerationInfoSize = Raven.Server.Dashboard.Cluster.Notifications.GcInfoPayload.GenerationInfoSize;

type MetricTab = "cpu" | "memory" | "gc";

interface PerformanceMetricsProps {
    summary: DebugPackageAnalysisSummary;
    nodeTag: string;
}

// Network/threads metrics need the dedicated debug-package endpoints; this widget covers the
// CPU / Memory / GC data already present in the summary payload.
export default function PerformanceMetrics({ summary, nodeTag }: PerformanceMetricsProps) {
    const node = summary.SummaryPerNode?.[nodeTag];
    const [tab, setTab] = useState<MetricTab>("cpu");

    const tabs: InputItem<MetricTab>[] = [
        { label: "CPU", value: "cpu" },
        { label: "Memory", value: "memory" },
        { label: "GC", value: "gc" },
    ];

    return (
        <div className="performance-metrics">
            <h3 className="mb-3">Performance Metrics</h3>
            <Card>
                <Card.Body className="vstack gap-3">
                    <MultiRadioToggle<MetricTab>
                        inputItems={tabs}
                        selectedItem={tab}
                        setSelectedItem={setTab}
                        label="Select metric"
                    />
                    {tab === "cpu" && <CpuTab cpu={node?.CpuUsageInfo} />}
                    {tab === "memory" && <MemoryTab memory={node?.MemoryUsageInfo} />}
                    {tab === "gc" && <GcTab gc={node?.GcInfo} />}
                </Card.Body>
            </Card>
        </div>
    );
}

function CpuTab({ cpu }: { cpu?: CpuUsageAnalysisInfo }) {
    if (!cpu) {
        return <EmptySet compact>No CPU data in the package</EmptySet>;
    }
    return (
        <>
            <div className="overview-stats d-flex gap-2 flex-wrap">
                <StatTile
                    label="Process CPU"
                    icon="processor"
                    iconColor="info"
                    value={formatPct(cpu.CurrentCpuUsage)}
                />
                <StatTile
                    label="Machine CPU"
                    icon="processor"
                    iconColor="warning"
                    value={formatPct(cpu.CurrentMachineCpuUsage)}
                />
                <StatTile label="Average CPU" icon="graph" value={formatPct(cpu.AverageCpuUsage)} />
                <StatTile label="Kernel time" icon="processor" value={formatPct(cpu.KernelTimePercentage)} />
                <StatTile label="Cores" icon="cluster-node" value={formatNum(cpu.NumberOfCores)} />
                <StatTile label="Utilized cores" icon="cluster-node" value={formatNum(cpu.UtilizedCores)} />
            </div>
            <ThreadList title="Top current CPU usage threads" threads={cpu.TopCurrentCpuUsageThreads} />
            <ThreadList title="Top overall CPU usage threads" threads={cpu.TopOverallCpuUsageThreads} />
        </>
    );
}

function ThreadList({ title, threads }: { title: string; threads: string[] }) {
    if (!threads || threads.length === 0) {
        return null;
    }
    return (
        <div>
            <div className="small-label ms-1 mb-1">{title}</div>
            <Table responsive className="m-0 align-middle">
                <tbody>
                    {threads.map((thread, index) => (
                        <tr key={index}>
                            <td className="text-break">{thread}</td>
                        </tr>
                    ))}
                </tbody>
            </Table>
        </div>
    );
}

function MemoryTab({ memory }: { memory?: MemoryAnalysisInfo }) {
    if (!memory) {
        return <EmptySet compact>No memory data in the package</EmptySet>;
    }
    const warnColor = memory.IsHighDirty ? "warning" : undefined;
    return (
        <>
            <div className="overview-stats d-flex gap-2 flex-wrap">
                <StatTile label="Working set" icon="memory" iconColor="info" value={memory.WorkingSet} />
                <StatTile label="Physical memory" icon="memory" value={memory.PhysicalMemory} />
                <StatTile label="Available memory" icon="memory" value={memory.AvailableMemory} />
                <StatTile label="Available for processing" icon="memory" value={memory.AvailableMemoryForProcessing} />
                <StatTile
                    label="Dirty memory"
                    icon="memory"
                    iconColor={warnColor}
                    value={memory.DirtyMemory}
                    valueColor={warnColor}
                />
                <StatTile label="Memory mapped" icon="storage" value={memory.MemoryMapped} />
            </div>
            <Table responsive className="m-0 align-middle">
                <tbody>
                    <MetricRow label="Managed allocations" value={memory.Managed?.ManagedAllocations} />
                    <MetricRow
                        label="Lucene managed allocations (term cache)"
                        value={memory.Managed?.LuceneManagedAllocationsForTermCache}
                    />
                    <MetricRow label="Unmanaged allocations" value={memory.Unmanaged?.UnmanagedAllocations} />
                    <MetricRow label="Encryption buffers in use" value={memory.Unmanaged?.EncryptionBuffersInUse} />
                    <MetricRow label="Encryption buffers pool" value={memory.Unmanaged?.EncryptionBuffersPool} />
                    <MetricRow label="Encryption locked memory" value={memory.Unmanaged?.EncryptionLockedMemory} />
                    <MetricRow
                        label="Lucene unmanaged allocations (sorting)"
                        value={memory.Unmanaged?.LuceneUnmanagedAllocationsForSorting}
                    />
                    <MetricRow
                        label="Lucene unmanaged allocations (term cache)"
                        value={memory.Unmanaged?.LuceneUnmanagedAllocationsForTermCache}
                    />
                </tbody>
            </Table>
        </>
    );
}

function MetricRow({ label, value }: { label: string; value: string }) {
    return (
        <tr>
            <td className="text-muted">{label}</td>
            <td className="text-end fw-bold">{value ?? "-"}</td>
        </tr>
    );
}

function GcTab({ gc }: { gc?: GcMemoryInfo }) {
    if (!gc) {
        return <EmptySet compact>No GC data in the package</EmptySet>;
    }
    const generations: { label: string; size: GenerationInfoSize }[] = [
        { label: "Gen 0", size: gc.Gen0HeapSize },
        { label: "Gen 1", size: gc.Gen1HeapSize },
        { label: "Gen 2", size: gc.Gen2HeapSize },
        { label: "Large object heap", size: gc.LargeObjectHeapSize },
        { label: "Pinned object heap", size: gc.PinnedObjectHeapSize },
    ];
    return (
        <>
            <div className="overview-stats d-flex gap-2 flex-wrap">
                <StatTile label="Last GC generation" icon="generation" value={`Gen ${gc.Generation}`} />
                <StatTile label="GC index" icon="refresh" value={formatNum(gc.Index)} />
                <StatTile label="Pause time" icon="clock" value={formatPct(gc.PauseTimePercentage)} />
                <StatTile
                    label="Total heap after"
                    icon="memory"
                    value={genUtils.formatBytesToSize(gc.TotalHeapSizeAfterBytes)}
                />
                <StatTile
                    label="Concurrent"
                    icon="refresh"
                    value={gc.Concurrent ? "Yes" : "No"}
                    valueColor={gc.Concurrent ? "success" : undefined}
                />
                <StatTile
                    label="Compacted"
                    icon="clean"
                    value={gc.Compacted ? "Yes" : "No"}
                    valueColor={gc.Compacted ? "success" : undefined}
                />
            </div>
            <div>
                <div className="small-label ms-1 mb-1">Heap by generation</div>
                <Table responsive className="m-0 align-middle">
                    <thead>
                        <tr>
                            <th>Generation</th>
                            <th>Size before</th>
                            <th>Size after</th>
                            <th>Fragmentation before</th>
                            <th>Fragmentation after</th>
                        </tr>
                    </thead>
                    <tbody>
                        {generations.map((generation) => (
                            <tr key={generation.label}>
                                <td className="fw-bold">{generation.label}</td>
                                <td>{genUtils.formatBytesToSize(generation.size?.SizeBeforeBytes ?? 0)}</td>
                                <td>{genUtils.formatBytesToSize(generation.size?.SizeAfterBytes ?? 0)}</td>
                                <td>{genUtils.formatBytesToSize(generation.size?.FragmentationBeforeBytes ?? 0)}</td>
                                <td>{genUtils.formatBytesToSize(generation.size?.FragmentationAfterBytes ?? 0)}</td>
                            </tr>
                        ))}
                    </tbody>
                </Table>
            </div>
            {gc.PauseDurationsInMs?.length > 0 && (
                <div className="text-muted">
                    Pause durations: {gc.PauseDurationsInMs.map((ms) => `${ms} ms`).join(", ")}
                </div>
            )}
        </>
    );
}

function formatPct(value: number | undefined): string {
    if (value == null) {
        return "-";
    }
    return `${Math.round(value)}%`;
}

function formatNum(value: number | undefined): string {
    if (value == null) {
        return "-";
    }
    return value.toLocaleString();
}

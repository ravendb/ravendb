import React, { useMemo } from "react";
import Card from "react-bootstrap/Card";
import Table from "react-bootstrap/Table";
import { EmptySet } from "components/common/EmptySet";
import NodeTagPill from "./NodeTagPill";
import { formatNumber, formatPercentage } from "./analyzerUtils";

type DebugPackageAnalysisSummary = Raven.Server.Documents.Handlers.Debugging.DebugPackage.DebugPackageAnalysisSummary;

interface ResourceRow {
    node: string;
    processCpu?: number;
    machineCpu?: number;
    cores?: number;
    workingSet?: string;
    availableMemory?: string;
    dirtyMemory?: string;
    isHighDirty: boolean;
    gcGeneration?: number;
    gcPause?: number;
}

interface ResourceUsageProps {
    summary: DebugPackageAnalysisSummary;
}

// Per-node CPU / Memory / GC comparison for the Cluster context - the triage view for spotting a hot
// or struggling node at a glance. PerformanceMetrics (Node context) has the per-node detail.
export default function ResourceUsage({ summary }: ResourceUsageProps) {
    const rows = useMemo(() => collectResourceRows(summary), [summary]);

    return (
        <div className="resource-usage">
            <h3 className="mb-3">Resource Usage</h3>
            <Card>
                <Card.Body>
                    {rows.length === 0 ? (
                        <EmptySet compact>No resource data in the package</EmptySet>
                    ) : (
                        <Table responsive className="m-0 align-middle">
                            <thead>
                                <tr>
                                    <th>Node</th>
                                    <th>Process CPU</th>
                                    <th>Machine CPU</th>
                                    <th>Cores</th>
                                    <th>Working set</th>
                                    <th>Available memory</th>
                                    <th>Dirty memory</th>
                                    <th>Last GC</th>
                                    <th>GC pause</th>
                                </tr>
                            </thead>
                            <tbody>
                                {rows.map((row) => (
                                    <tr key={row.node}>
                                        <td>
                                            <NodeTagPill tag={row.node} />
                                        </td>
                                        <td>{formatPercentage(row.processCpu)}</td>
                                        <td>{formatPercentage(row.machineCpu)}</td>
                                        <td>{formatNumber(row.cores)}</td>
                                        <td>{row.workingSet ?? "-"}</td>
                                        <td>{row.availableMemory ?? "-"}</td>
                                        <td className={row.isHighDirty ? "text-warning" : ""}>
                                            {row.dirtyMemory ?? "-"}
                                        </td>
                                        <td>{row.gcGeneration != null ? `Gen ${row.gcGeneration}` : "-"}</td>
                                        <td>{formatPercentage(row.gcPause)}</td>
                                    </tr>
                                ))}
                            </tbody>
                        </Table>
                    )}
                </Card.Body>
            </Card>
        </div>
    );
}

function collectResourceRows(summary: DebugPackageAnalysisSummary): ResourceRow[] {
    const rows: ResourceRow[] = [];

    Object.entries(summary.SummaryPerNode ?? {}).forEach(([nodeTag, node]) => {
        const cpu = node.CpuUsageInfo;
        const memory = node.MemoryUsageInfo;
        const gc = node.GcInfo;
        rows.push({
            node: nodeTag,
            processCpu: cpu?.CurrentCpuUsage,
            machineCpu: cpu?.CurrentMachineCpuUsage,
            cores: cpu?.NumberOfCores,
            workingSet: memory?.WorkingSet,
            availableMemory: memory?.AvailableMemory,
            dirtyMemory: memory?.DirtyMemory,
            isHighDirty: memory?.IsHighDirty ?? false,
            gcGeneration: gc?.Generation,
            gcPause: gc?.PauseTimePercentage,
        });
    });

    return rows.sort((a, b) => a.node.localeCompare(b.node));
}

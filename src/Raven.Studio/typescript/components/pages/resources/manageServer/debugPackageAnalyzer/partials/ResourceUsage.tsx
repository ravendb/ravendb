import React, { useMemo } from "react";
import Card from "react-bootstrap/Card";
import Table from "react-bootstrap/Table";
import { EmptySet } from "components/common/EmptySet";
import NodeTagPill from "./NodeTagPill";
import { formatNumber, formatPercentage } from "./analyzerUtils";
import { SortableHeader, useSortableData } from "./sortableTable";

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

// memory columns are server-formatted strings ("1.2 GB"), so only the numeric columns are sortable
const resourceSortAccessors: Record<string, (row: ResourceRow) => number | string> = {
    node: (row) => row.node,
    processCpu: (row) => row.processCpu ?? 0,
    machineCpu: (row) => row.machineCpu ?? 0,
    cores: (row) => row.cores ?? 0,
    lastGc: (row) => row.gcGeneration ?? -1,
    gcPause: (row) => row.gcPause ?? 0,
};

// Per-node CPU / Memory / GC comparison for the Cluster context - the triage view for spotting a hot
// or struggling node at a glance. PerformanceMetrics (Node context) has the per-node detail.
export default function ResourceUsage({ summary }: ResourceUsageProps) {
    const rows = useMemo(() => collectResourceRows(summary), [summary]);
    const { sorted, sortKey, sortDirection, requestSort } = useSortableData(rows, resourceSortAccessors, "processCpu");
    const sortProps = { sortKey, sortDirection, onSort: requestSort };

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
                                    <SortableHeader label="Node" columnKey="node" {...sortProps} />
                                    <SortableHeader label="Process CPU" columnKey="processCpu" {...sortProps} />
                                    <SortableHeader label="Machine CPU" columnKey="machineCpu" {...sortProps} />
                                    <SortableHeader label="Cores" columnKey="cores" {...sortProps} />
                                    <th>Working set</th>
                                    <th>Available memory</th>
                                    <th>Dirty memory</th>
                                    <SortableHeader label="Last GC" columnKey="lastGc" {...sortProps} />
                                    <SortableHeader label="GC pause" columnKey="gcPause" {...sortProps} />
                                </tr>
                            </thead>
                            <tbody>
                                {sorted.map((row) => (
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

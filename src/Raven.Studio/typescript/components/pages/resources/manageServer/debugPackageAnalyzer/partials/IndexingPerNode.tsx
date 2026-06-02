import React, { useMemo } from "react";
import Card from "react-bootstrap/Card";
import Table from "react-bootstrap/Table";
import NodeTagPill from "./NodeTagPill";
import { EmptySet } from "components/common/EmptySet";
import { SortableHeader, useSortableData } from "./sortableTable";

type DebugPackageAnalysisSummary = Raven.Server.Documents.Handlers.Debugging.DebugPackage.DebugPackageAnalysisSummary;

interface IndexingRow {
    node: string;
    indexed: number;
    mapped: number;
    reduced: number;
}

interface IndexingPerNodeProps {
    summary: DebugPackageAnalysisSummary;
    nodeTag?: string;
}

const indexingSortAccessors: Record<string, (row: IndexingRow) => number | string> = {
    node: (row) => row.node,
    indexed: (row) => row.indexed ?? 0,
    mapped: (row) => row.mapped ?? 0,
    reduced: (row) => row.reduced ?? 0,
};

// the summary exposes indexing speed as a per-node aggregate (no per-database breakdown),
// so this shows one row per node rather than per database
export default function IndexingPerNode({ summary, nodeTag }: IndexingPerNodeProps) {
    const rows = useMemo(() => collectIndexingRows(summary, nodeTag), [summary, nodeTag]);
    const { sorted, sortKey, sortDirection, requestSort } = useSortableData(rows, indexingSortAccessors, "node", "asc");
    const sortProps = { sortKey, sortDirection, onSort: requestSort };

    return (
        <div className="indexing-per-node flex-grow-1">
            <h3 className="mb-3">Indexing per Node</h3>
            <Card>
                <Card.Body>
                    {rows.length === 0 ? (
                        <EmptySet compact>No indexing data in the package</EmptySet>
                    ) : (
                        <Table responsive className="m-0 align-middle">
                            <thead>
                                <tr>
                                    <SortableHeader label="Node" columnKey="node" {...sortProps} />
                                    <SortableHeader label="Indexed/s" columnKey="indexed" {...sortProps} />
                                    <SortableHeader label="Mapped/s" columnKey="mapped" {...sortProps} />
                                    <SortableHeader label="Reduced/s" columnKey="reduced" {...sortProps} />
                                </tr>
                            </thead>
                            <tbody>
                                {sorted.map((row) => (
                                    <tr key={row.node}>
                                        <td>
                                            <NodeTagPill tag={row.node} />
                                        </td>
                                        <td>{formatRate(row.indexed)}</td>
                                        <td>{formatRate(row.mapped)}</td>
                                        <td>{formatRate(row.reduced)}</td>
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

function formatRate(value: number): string {
    if (value == null) {
        return "-";
    }
    if (value === 0) {
        return "0";
    }
    if (value >= 1) {
        return Math.round(value).toLocaleString();
    }
    return value.toFixed(2);
}

function collectIndexingRows(summary: DebugPackageAnalysisSummary, nodeTag?: string): IndexingRow[] {
    const rows: IndexingRow[] = [];

    Object.entries(summary.SummaryPerNode ?? {}).forEach(([tag, node]) => {
        if (nodeTag && tag !== nodeTag) {
            return;
        }
        const speed = node.DatabaseIndexingSpeed;
        if (speed) {
            rows.push({
                node: tag,
                indexed: speed.IndexedPerSecond,
                mapped: speed.MappedPerSecond,
                reduced: speed.ReducedPerSecond,
            });
        }
    });

    return rows.sort((a, b) => a.node.localeCompare(b.node));
}

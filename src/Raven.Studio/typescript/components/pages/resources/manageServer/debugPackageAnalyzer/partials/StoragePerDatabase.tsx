import React, { useMemo } from "react";
import Card from "react-bootstrap/Card";
import Table from "react-bootstrap/Table";
import { StatePill } from "components/common/StatePill";
import { EmptySet } from "components/common/EmptySet";
import genUtils from "common/generalUtils";

type DebugPackageAnalysisSummary = Raven.Server.Documents.Handlers.Debugging.DebugPackage.DebugPackageAnalysisSummary;

interface StorageRow {
    key: string;
    database: string;
    node: string;
    size: number;
    temp: number;
}

interface StoragePerDatabaseProps {
    summary: DebugPackageAnalysisSummary;
    nodeTag?: string;
}

export default function StoragePerDatabase({ summary, nodeTag }: StoragePerDatabaseProps) {
    const rows = useMemo(() => collectStorageRows(summary, nodeTag), [summary, nodeTag]);

    return (
        <div className="storage-per-database flex-grow-1">
            <h3 className="mb-3">Storage per Database</h3>
            <Card>
                <Card.Body>
                    {rows.length === 0 ? (
                        <EmptySet compact>No storage data in the package</EmptySet>
                    ) : (
                        <Table responsive className="m-0 align-middle">
                            <thead>
                                <tr>
                                    <th>Database</th>
                                    <th>Node</th>
                                    <th>Data</th>
                                    <th>Temp</th>
                                    <th>Total</th>
                                </tr>
                            </thead>
                            <tbody>
                                {rows.map((row) => (
                                    <tr key={row.key}>
                                        <td className="fw-bold">{row.database}</td>
                                        <td>
                                            <StatePill bg="node">{row.node}</StatePill>
                                        </td>
                                        <td>{genUtils.formatBytesToSize(row.size)}</td>
                                        <td>{genUtils.formatBytesToSize(row.temp)}</td>
                                        <td>{genUtils.formatBytesToSize(row.size + row.temp)}</td>
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

function collectStorageRows(summary: DebugPackageAnalysisSummary, nodeTag?: string): StorageRow[] {
    const rows: StorageRow[] = [];

    Object.entries(summary.SummaryPerNode ?? {}).forEach(([tag, node]) => {
        if (nodeTag && tag !== nodeTag) {
            return;
        }
        (node.DatabaseStorageUsage?.Items ?? []).forEach((item) => {
            rows.push({
                key: `${item.Database}-${tag}`,
                database: item.Database,
                node: tag,
                size: item.Size,
                temp: item.TempBuffersSize,
            });
        });
    });

    return rows.sort((a, b) => a.database.localeCompare(b.database) || a.node.localeCompare(b.node));
}

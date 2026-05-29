import React, { useMemo } from "react";
import Card from "react-bootstrap/Card";
import Table from "react-bootstrap/Table";
import { EmptySet } from "components/common/EmptySet";
import { StatePill } from "components/common/StatePill";
import { Icon } from "components/common/Icon";
import NodeTagPill from "./NodeTagPill";
import StatTile from "./StatTile";
import genUtils from "common/generalUtils";

type DebugPackageAnalysisSummary = Raven.Server.Documents.Handlers.Debugging.DebugPackage.DebugPackageAnalysisSummary;

interface DatabaseContextViewProps {
    summary: DebugPackageAnalysisSummary;
    database: string;
}

interface OverviewRow {
    node: string;
    online: boolean;
    disabled: boolean;
    documents: number;
    indexes: number;
    erroredIndexes: number;
    indexingErrors: number;
    ongoingTasks: number;
    alerts: number;
    performanceHints: number;
    lastBackup: string;
}

interface StorageRow {
    node: string;
    data: number;
    temp: number;
}

// Per-node view of a single database. Documents/indexes/storage are reported per node, so this
// surfaces divergence between nodes (replication lag, node-local indexing errors, uneven storage).
// Per-task-type and per-database indexing speed are not in the summary (node-scoped only).
export default function DatabaseContextView({ summary, database }: DatabaseContextViewProps) {
    const overviewRows = useMemo(() => collectOverviewRows(summary, database), [summary, database]);
    const storageRows = useMemo(() => collectStorageRows(summary, database), [summary, database]);

    const totalData = storageRows.reduce((sum, row) => sum + row.data, 0);
    const totalTemp = storageRows.reduce((sum, row) => sum + row.temp, 0);

    if (overviewRows.length === 0 && storageRows.length === 0) {
        return <EmptySet>No data for {database} in the package</EmptySet>;
    }

    return (
        <div className="database-context vstack gap-4">
            <div>
                <h3 className="mb-3">Database Overview</h3>
                <Card>
                    <Card.Body>
                        {overviewRows.length === 0 ? (
                            <EmptySet compact>No database overview data in the package</EmptySet>
                        ) : (
                            <Table responsive className="m-0 align-middle">
                                <thead>
                                    <tr>
                                        <th>Node</th>
                                        <th>State</th>
                                        <th>Documents</th>
                                        <th>Indexes</th>
                                        <th>Indexing errors</th>
                                        <th>Ongoing tasks</th>
                                        <th>Alerts</th>
                                        <th>Perf. hints</th>
                                        <th>Last backup</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {overviewRows.map((row) => (
                                        <tr key={row.node}>
                                            <td>
                                                <NodeTagPill tag={row.node} />
                                            </td>
                                            <td>
                                                {row.disabled ? (
                                                    <StatePill bg="warning">Disabled</StatePill>
                                                ) : (
                                                    <StatePill bg="success">Online</StatePill>
                                                )}
                                            </td>
                                            <td>{formatCount(row.documents)}</td>
                                            <td>
                                                {formatCount(row.indexes)}
                                                {row.erroredIndexes > 0 && (
                                                    <span className="text-danger ms-1">
                                                        <Icon icon="danger" margin="m-0" /> {row.erroredIndexes}
                                                    </span>
                                                )}
                                            </td>
                                            <td className={row.indexingErrors > 0 ? "text-danger" : ""}>
                                                {formatCount(row.indexingErrors)}
                                            </td>
                                            <td>{formatCount(row.ongoingTasks)}</td>
                                            <td className={row.alerts > 0 ? "text-warning" : ""}>
                                                {formatCount(row.alerts)}
                                            </td>
                                            <td>{formatCount(row.performanceHints)}</td>
                                            <td>{row.lastBackup}</td>
                                        </tr>
                                    ))}
                                </tbody>
                            </Table>
                        )}
                    </Card.Body>
                </Card>
            </div>

            <div>
                <h3 className="mb-3">Storage Overview</h3>
                <Card>
                    <Card.Body className="vstack gap-3">
                        <div className="overview-stats d-flex gap-3 flex-wrap">
                            <StatTile
                                label="Data size"
                                icon="storage"
                                iconColor="info"
                                value={genUtils.formatBytesToSize(totalData)}
                            />
                            <StatTile label="Temp size" icon="storage" value={genUtils.formatBytesToSize(totalTemp)} />
                            <StatTile
                                label="Total size"
                                icon="storage"
                                value={genUtils.formatBytesToSize(totalData + totalTemp)}
                            />
                        </div>
                        {storageRows.length === 0 ? (
                            <EmptySet compact>No storage data in the package</EmptySet>
                        ) : (
                            <Table responsive className="m-0 align-middle">
                                <thead>
                                    <tr>
                                        <th>Node</th>
                                        <th>Data</th>
                                        <th>Temp</th>
                                        <th>Total</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {storageRows.map((row) => (
                                        <tr key={row.node}>
                                            <td>
                                                <NodeTagPill tag={row.node} />
                                            </td>
                                            <td>{genUtils.formatBytesToSize(row.data)}</td>
                                            <td>{genUtils.formatBytesToSize(row.temp)}</td>
                                            <td>{genUtils.formatBytesToSize(row.data + row.temp)}</td>
                                        </tr>
                                    ))}
                                </tbody>
                            </Table>
                        )}
                    </Card.Body>
                </Card>
            </div>
        </div>
    );
}

function formatCount(value: number): string {
    if (value == null || value < 0) {
        return "-";
    }
    return value.toLocaleString();
}

function collectOverviewRows(summary: DebugPackageAnalysisSummary, database: string): OverviewRow[] {
    const rows: OverviewRow[] = [];

    Object.entries(summary.SummaryPerNode ?? {}).forEach(([nodeTag, node]) => {
        (node.DatabasesOverview?.Items ?? []).forEach((item) => {
            if (item.Database !== database || item.Irrelevant) {
                return;
            }
            const lastBackup = item.BackupInfo?.LastBackup;
            rows.push({
                node: nodeTag,
                online: item.Online,
                disabled: item.Disabled,
                documents: item.DocumentsCount,
                indexes: item.IndexesCount,
                erroredIndexes: item.ErroredIndexesCount,
                indexingErrors: item.IndexingErrorsCount,
                ongoingTasks: item.OngoingTasksCount,
                alerts: item.AlertsCount,
                performanceHints: item.PerformanceHintsCount,
                lastBackup: lastBackup ? genUtils.formatUtcDateAsLocal(lastBackup) : "Never",
            });
        });
    });

    return rows.sort((a, b) => a.node.localeCompare(b.node));
}

function collectStorageRows(summary: DebugPackageAnalysisSummary, database: string): StorageRow[] {
    const rows: StorageRow[] = [];

    Object.entries(summary.SummaryPerNode ?? {}).forEach(([nodeTag, node]) => {
        (node.DatabaseStorageUsage?.Items ?? []).forEach((item) => {
            if (item.Database !== database) {
                return;
            }
            rows.push({ node: nodeTag, data: item.Size, temp: item.TempBuffersSize });
        });
    });

    return rows.sort((a, b) => a.node.localeCompare(b.node));
}

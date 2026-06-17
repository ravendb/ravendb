import React, { memo, useMemo } from "react";
import Badge from "react-bootstrap/Badge";
import { EmptySet } from "components/common/EmptySet";
import { Icon } from "components/common/Icon";
import NodeTagPill from "./NodeTagPill";
import StatTile from "./StatTile";
import DatabaseStats from "./DatabaseStats";
import DatabaseOngoingTasks from "./DatabaseOngoingTasks";
import DatabaseIndexStats from "./DatabaseIndexStats";
import DatabaseIndexPerformanceLink from "./DatabaseIndexPerformanceLink";
import DatabaseIndexDefinitions from "./DatabaseIndexDefinitions";
import DatabaseIndexErrors from "./DatabaseIndexErrors";
import DatabaseSettings from "./DatabaseSettings";
import AnalysisSection from "./AnalysisSection";
import genUtils from "common/generalUtils";

type DebugPackageAnalysisSummary = Raven.Server.Documents.Handlers.Debugging.DebugPackage.DebugPackageAnalysisSummary;

interface DatabaseContextViewProps {
    summary: DebugPackageAnalysisSummary;
    database: string;
    selectedNode: string;
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
export default memo(function DatabaseContextView({ summary, database, selectedNode }: DatabaseContextViewProps) {
    const overviewRows = useMemo(() => collectOverviewRows(summary, database), [summary, database]);
    const storageRows = useMemo(() => collectStorageRows(summary, database), [summary, database]);
    const overviewNodeTags = useMemo(() => overviewRows.map((row) => row.node), [overviewRows]);

    const totalData = storageRows.reduce((sum, row) => sum + row.data, 0);
    const totalTemp = storageRows.reduce((sum, row) => sum + row.temp, 0);

    if (overviewRows.length === 0 && storageRows.length === 0) {
        return <EmptySet>No data for {database} in the package</EmptySet>;
    }

    return (
        <div className="database-context vstack gap-3">
            <h2 className="database-context-group-heading">Global</h2>

            <AnalysisSection id="database-overview" label="Database Overview" group="Global">
                <div className="panel-bg-1 rounded">
                    <div className="p-4 vstack gap-3">
                        <h3 className="mb-0">Database Overview</h3>
                        {overviewRows.length === 0 ? (
                            <EmptySet compact className="justify-content-center">
                                No database overview data in the package
                            </EmptySet>
                        ) : (
                            <div className="vstack gap-3">
                                {overviewRows.map((row) => (
                                    <div key={row.node}>
                                        <div className="hstack gap-1 align-items-center mb-1">
                                            Node <NodeTagPill tag={row.node} />
                                        </div>
                                        <div className="overview-stats gap-2">
                                            <StatTile
                                                label="Node status"
                                                icon={row.disabled ? "cancel" : "check"}
                                                iconColor={row.disabled ? "warning" : "success"}
                                                value={row.disabled ? "Disabled" : "Online"}
                                            />
                                            <StatTile
                                                label="Documents"
                                                icon="documents"
                                                value={formatCount(row.documents)}
                                            />
                                            <StatTile
                                                label="Indexes"
                                                icon="indexing"
                                                value={
                                                    row.erroredIndexes > 0 ? (
                                                        <>
                                                            {formatCount(row.indexes)}{" "}
                                                            <Badge bg="danger" pill>
                                                                <Icon icon="danger" margin="m-0" /> {row.erroredIndexes}
                                                            </Badge>
                                                        </>
                                                    ) : (
                                                        formatCount(row.indexes)
                                                    )
                                                }
                                            />
                                            <StatTile
                                                label="Indexing errors"
                                                icon="indexing"
                                                iconColor={row.indexingErrors > 0 ? "danger" : undefined}
                                                valueColor={row.indexingErrors > 0 ? "danger" : undefined}
                                                value={formatCount(row.indexingErrors)}
                                            />
                                            <StatTile
                                                label="Ongoing tasks"
                                                icon="ongoing-tasks"
                                                value={formatCount(row.ongoingTasks)}
                                            />
                                            <StatTile
                                                label="Alerts"
                                                icon="alerts"
                                                iconColor={row.alerts > 0 ? "warning" : undefined}
                                                valueColor={row.alerts > 0 ? "warning" : undefined}
                                                value={formatCount(row.alerts)}
                                            />
                                            <StatTile
                                                label="Perf. hints"
                                                icon="performance"
                                                value={formatCount(row.performanceHints)}
                                            />
                                            <StatTile label="Last backup" icon="backup" value={row.lastBackup} />
                                        </div>
                                    </div>
                                ))}
                            </div>
                        )}
                    </div>
                </div>
            </AnalysisSection>

            <AnalysisSection id="database-storage" label="Storage Overview" group="Global">
                <div className="panel-bg-1 rounded">
                    <div className="p-4 vstack gap-3">
                        <h3 className="mb-0">Storage Overview</h3>
                        <div className="overview-stats gap-2">
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
                            <EmptySet compact className="justify-content-center">
                                No storage data in the package
                            </EmptySet>
                        ) : (
                            <div className="vstack gap-3">
                                {storageRows.map((row) => (
                                    <div key={row.node}>
                                        <div className="hstack gap-1 align-items-center mb-1">
                                            Node <NodeTagPill tag={row.node} />
                                        </div>
                                        <div className="overview-stats gap-2">
                                            <StatTile
                                                label="Data size"
                                                icon="storage"
                                                iconColor="info"
                                                value={genUtils.formatBytesToSize(row.data)}
                                            />
                                            <StatTile
                                                label="Temp size"
                                                icon="storage"
                                                value={genUtils.formatBytesToSize(row.temp)}
                                            />
                                            <StatTile
                                                label="Total size"
                                                icon="storage"
                                                value={genUtils.formatBytesToSize(row.data + row.temp)}
                                            />
                                        </div>
                                    </div>
                                ))}
                            </div>
                        )}
                    </div>
                </div>
            </AnalysisSection>

            <h2 className="database-context-group-heading">Node-scoped</h2>

            <AnalysisSection id="database-stats" label="Statistics" group="Node-scoped">
                <DatabaseStats packageId={summary.PackageId} database={database} node={selectedNode} />
            </AnalysisSection>

            <AnalysisSection id="database-index-stats" label="Index Stats" group="Node-scoped">
                <DatabaseIndexStats packageId={summary.PackageId} database={database} node={selectedNode} />
            </AnalysisSection>

            <AnalysisSection id="database-index-performance" label="Indexing Performance" group="Node-scoped">
                <DatabaseIndexPerformanceLink packageId={summary.PackageId} database={database} node={selectedNode} />
            </AnalysisSection>

            <AnalysisSection id="database-index-definitions" label="Index Definitions" group="Node-scoped">
                <DatabaseIndexDefinitions packageId={summary.PackageId} database={database} node={selectedNode} />
            </AnalysisSection>

            <AnalysisSection id="database-index-errors" label="Index Errors" group="Node-scoped">
                <DatabaseIndexErrors packageId={summary.PackageId} database={database} node={selectedNode} />
            </AnalysisSection>

            <AnalysisSection id="database-ongoing-tasks" label="Ongoing Tasks" group="Node-scoped">
                <DatabaseOngoingTasks
                    packageId={summary.PackageId}
                    database={database}
                    nodes={overviewNodeTags}
                    selectedNode={selectedNode}
                />
            </AnalysisSection>

            <AnalysisSection id="database-settings" label="Settings" group="Node-scoped">
                <DatabaseSettings packageId={summary.PackageId} database={database} node={selectedNode} />
            </AnalysisSection>
        </div>
    );
});

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

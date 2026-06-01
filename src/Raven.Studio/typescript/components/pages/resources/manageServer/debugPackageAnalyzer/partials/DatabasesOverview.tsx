import React, { useMemo } from "react";
import Card from "react-bootstrap/Card";
import Table from "react-bootstrap/Table";
import { Icon } from "components/common/Icon";
import { StatePill } from "components/common/StatePill";
import NodeTagPill from "./NodeTagPill";
import { SortableHeader, useSortableData } from "./sortableTable";

type DebugPackageAnalysisSummary = Raven.Server.Documents.Handlers.Debugging.DebugPackage.DebugPackageAnalysisSummary;

interface DatabasesOverviewProps {
    summary: DebugPackageAnalysisSummary;
}

interface AggregatedDatabase {
    database: string;
    documentsCount: number;
    indexesCount: number;
    erroredIndexesCount: number;
    indexingErrorsCount: number;
    ongoingTasksCount: number;
    replicationFactor: number;
    disabled: boolean;
    nodes: string[];
}

const databasesSortAccessors: Record<string, (db: AggregatedDatabase) => number | string> = {
    database: (db) => db.database,
    documents: (db) => db.documentsCount ?? 0,
    indexes: (db) => db.indexesCount ?? 0,
    indexingErrors: (db) => db.indexingErrorsCount ?? 0,
    ongoingTasks: (db) => db.ongoingTasksCount ?? 0,
    replicationFactor: (db) => db.replicationFactor ?? 0,
    state: (db) => (db.disabled ? 1 : 0),
};

export default function DatabasesOverview({ summary }: DatabasesOverviewProps) {
    const databases = useMemo(() => aggregateDatabases(summary), [summary]);
    const { sorted, sortKey, sortDirection, requestSort } = useSortableData(
        databases,
        databasesSortAccessors,
        "documents"
    );
    const sortProps = { sortKey, sortDirection, onSort: requestSort };

    const disabledCount = databases.filter((d) => d.disabled).length;
    const onlineCount = databases.length - disabledCount;

    return (
        <div className="databases-overview">
            <h3 className="mb-3">Databases Overview</h3>
            <Card>
                <Card.Body className="vstack gap-3">
                    <div className="hstack gap-4 flex-wrap">
                        <span className="hstack gap-1">
                            <Icon icon="database" margin="m-0" /> {databases.length} total
                        </span>
                        <span className="hstack gap-1 text-success">
                            <Icon icon="database" addon="check" margin="m-0" /> {onlineCount} online
                        </span>
                        <span className="hstack gap-1 text-warning">
                            <Icon icon="database" addon="cancel" margin="m-0" /> {disabledCount} disabled
                        </span>
                    </div>

                    {databases.length === 0 ? (
                        <div className="text-muted">No databases found in the package</div>
                    ) : (
                        <Table responsive className="m-0 align-middle">
                            <thead>
                                <tr>
                                    <SortableHeader label="Database" columnKey="database" {...sortProps} />
                                    <SortableHeader label="Documents" columnKey="documents" {...sortProps} />
                                    <SortableHeader label="Indexes" columnKey="indexes" {...sortProps} />
                                    <SortableHeader label="Indexing errors" columnKey="indexingErrors" {...sortProps} />
                                    <SortableHeader label="Ongoing tasks" columnKey="ongoingTasks" {...sortProps} />
                                    <SortableHeader
                                        label="Replication factor"
                                        columnKey="replicationFactor"
                                        {...sortProps}
                                    />
                                    <th>Nodes</th>
                                    <SortableHeader label="State" columnKey="state" {...sortProps} />
                                </tr>
                            </thead>
                            <tbody>
                                {sorted.map((db) => (
                                    <tr key={db.database}>
                                        <td className="fw-bold">{db.database}</td>
                                        <td>{formatCount(db.documentsCount)}</td>
                                        <td>
                                            {formatCount(db.indexesCount)}
                                            {db.erroredIndexesCount > 0 && (
                                                <span className="text-danger ms-1">
                                                    <Icon icon="danger" margin="m-0" /> {db.erroredIndexesCount}
                                                </span>
                                            )}
                                        </td>
                                        <td className={db.indexingErrorsCount > 0 ? "text-danger" : ""}>
                                            {formatCount(db.indexingErrorsCount)}
                                        </td>
                                        <td>{formatCount(db.ongoingTasksCount)}</td>
                                        <td>{formatCount(db.replicationFactor)}</td>
                                        <td>
                                            <div className="hstack gap-1 flex-wrap">
                                                {db.nodes.map((nodeTag) => (
                                                    <NodeTagPill key={nodeTag} tag={nodeTag} />
                                                ))}
                                            </div>
                                        </td>
                                        <td>
                                            {db.disabled ? (
                                                <StatePill bg="warning">Disabled</StatePill>
                                            ) : (
                                                <StatePill bg="success">Online</StatePill>
                                            )}
                                        </td>
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

function formatCount(value: number): string {
    if (value == null || value < 0) {
        return "-";
    }
    return value.toLocaleString();
}

// the package reports each database per node; merge into one row per database with the representative
// (largest, to tolerate replication lag / unavailable -1 values) counts and the set of nodes hosting it
function aggregateDatabases(summary: DebugPackageAnalysisSummary): AggregatedDatabase[] {
    const map = new Map<string, AggregatedDatabase>();

    Object.entries(summary.SummaryPerNode ?? {}).forEach(([nodeTag, node]) => {
        (node.DatabasesOverview?.Items ?? []).forEach((item) => {
            if (item.Irrelevant) {
                return;
            }

            let agg = map.get(item.Database);
            if (!agg) {
                agg = {
                    database: item.Database,
                    documentsCount: item.DocumentsCount,
                    indexesCount: item.IndexesCount,
                    erroredIndexesCount: item.ErroredIndexesCount,
                    indexingErrorsCount: item.IndexingErrorsCount,
                    ongoingTasksCount: item.OngoingTasksCount,
                    replicationFactor: item.ReplicationFactor,
                    disabled: item.Disabled,
                    nodes: [],
                };
                map.set(item.Database, agg);
            } else {
                agg.documentsCount = Math.max(agg.documentsCount, item.DocumentsCount);
                agg.indexesCount = Math.max(agg.indexesCount, item.IndexesCount);
                agg.erroredIndexesCount = Math.max(agg.erroredIndexesCount, item.ErroredIndexesCount);
                agg.indexingErrorsCount = Math.max(agg.indexingErrorsCount, item.IndexingErrorsCount);
                agg.ongoingTasksCount = Math.max(agg.ongoingTasksCount, item.OngoingTasksCount);
                agg.replicationFactor = Math.max(agg.replicationFactor, item.ReplicationFactor);
            }

            if (!agg.nodes.includes(nodeTag)) {
                agg.nodes.push(nodeTag);
            }
        });
    });

    return Array.from(map.values()).sort((a, b) => a.database.localeCompare(b.database));
}

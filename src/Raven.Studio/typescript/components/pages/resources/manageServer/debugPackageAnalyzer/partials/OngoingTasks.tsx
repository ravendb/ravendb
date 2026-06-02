import React, { useMemo } from "react";
import Card from "react-bootstrap/Card";
import Table from "react-bootstrap/Table";
import NodeTagPill from "./NodeTagPill";
import { EmptySet } from "components/common/EmptySet";
import { SortableHeader, useSortableData } from "./sortableTable";

type DebugPackageAnalysisSummary = Raven.Server.Documents.Handlers.Debugging.DebugPackage.DebugPackageAnalysisSummary;
type DatabaseOngoingTasksInfoItem = Raven.Server.Dashboard.DatabaseOngoingTasksInfoItem;

// the summary reports task counts per node (the Database field is not populated), so we aggregate by task type
const taskTypeLabels: { field: keyof DatabaseOngoingTasksInfoItem; label: string }[] = [
    { field: "ExternalReplicationCount", label: "External Replication" },
    { field: "ReplicationHubCount", label: "Replication Hub" },
    { field: "ReplicationSinkCount", label: "Replication Sink" },
    { field: "RavenEtlCount", label: "RavenDB ETL" },
    { field: "SqlEtlCount", label: "SQL ETL" },
    { field: "OlapEtlCount", label: "OLAP ETL" },
    { field: "ElasticSearchEtlCount", label: "Elasticsearch ETL" },
    { field: "KafkaEtlCount", label: "Kafka ETL" },
    { field: "RabbitMqEtlCount", label: "RabbitMQ ETL" },
    { field: "AzureQueueStorageEtlCount", label: "Azure Queue Storage ETL" },
    { field: "AmazonSqsEtlCount", label: "Amazon SQS ETL" },
    { field: "SnowflakeEtlCount", label: "Snowflake ETL" },
    { field: "KafkaSinkCount", label: "Kafka Sink" },
    { field: "RabbitMqSinkCount", label: "RabbitMQ Sink" },
    { field: "PeriodicBackupCount", label: "Backup" },
    { field: "SubscriptionCount", label: "Subscription" },
    { field: "EmbeddingsGenerationCount", label: "Embeddings Generation" },
    { field: "GenAiCount", label: "GenAI" },
];

interface TaskRow {
    label: string;
    count: number;
    nodes: string[];
}

interface OngoingTasksProps {
    summary: DebugPackageAnalysisSummary;
    nodeTag?: string;
}

const taskSortAccessors: Record<string, (row: TaskRow) => number | string> = {
    label: (row) => row.label,
    count: (row) => row.count,
};

export default function OngoingTasks({ summary, nodeTag }: OngoingTasksProps) {
    const rows = useMemo(() => aggregateTasks(summary, nodeTag), [summary, nodeTag]);
    const { sorted, sortKey, sortDirection, requestSort } = useSortableData(rows, taskSortAccessors, "count");
    const sortProps = { sortKey, sortDirection, onSort: requestSort };

    const total = rows.reduce((sum, row) => sum + row.count, 0);

    return (
        <div className="ongoing-tasks">
            <h3 className="mb-3">Ongoing Tasks</h3>
            <Card>
                <Card.Body className="vstack gap-3">
                    <div className="text-muted">{total} active ongoing tasks</div>
                    {rows.length === 0 ? (
                        <EmptySet compact>No ongoing tasks in the package</EmptySet>
                    ) : (
                        <Table responsive className="m-0 align-middle">
                            <thead>
                                <tr>
                                    <SortableHeader label="Task" columnKey="label" {...sortProps} />
                                    <SortableHeader label="Count" columnKey="count" {...sortProps} />
                                    <th>Nodes</th>
                                </tr>
                            </thead>
                            <tbody>
                                {sorted.map((row) => (
                                    <tr key={row.label}>
                                        <td className="fw-bold">{row.label}</td>
                                        <td>{row.count.toLocaleString()}</td>
                                        <td>
                                            <div className="hstack gap-1 flex-wrap">
                                                {row.nodes.map((node) => (
                                                    <NodeTagPill key={node} tag={node} />
                                                ))}
                                            </div>
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

function aggregateTasks(summary: DebugPackageAnalysisSummary, nodeTag?: string): TaskRow[] {
    const byType = new Map<string, TaskRow>();

    Object.entries(summary.SummaryPerNode ?? {}).forEach(([tag, node]) => {
        if (nodeTag && tag !== nodeTag) {
            return;
        }
        (node.DatabasesOngoingTasks?.Items ?? []).forEach((item) => {
            taskTypeLabels.forEach(({ field, label }) => {
                const count = item[field] as number;
                if (count > 0) {
                    let row = byType.get(label);
                    if (!row) {
                        row = { label, count: 0, nodes: [] };
                        byType.set(label, row);
                    }
                    row.count += count;
                    if (!row.nodes.includes(tag)) {
                        row.nodes.push(tag);
                    }
                }
            });
        });
    });

    const rows = Array.from(byType.values());
    rows.forEach((row) => row.nodes.sort());
    return rows.sort((a, b) => a.label.localeCompare(b.label));
}

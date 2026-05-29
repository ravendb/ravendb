import React, { useMemo } from "react";
import Card from "react-bootstrap/Card";
import Table from "react-bootstrap/Table";
import { StatePill } from "components/common/StatePill";
import { EmptySet } from "components/common/EmptySet";

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

export default function OngoingTasks({ summary, nodeTag }: OngoingTasksProps) {
    const rows = useMemo(() => aggregateTasks(summary, nodeTag), [summary, nodeTag]);

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
                                    <th>Task</th>
                                    <th>Count</th>
                                    <th>Nodes</th>
                                </tr>
                            </thead>
                            <tbody>
                                {rows.map((row) => (
                                    <tr key={row.label}>
                                        <td className="fw-bold">{row.label}</td>
                                        <td>{row.count.toLocaleString()}</td>
                                        <td>
                                            <div className="hstack gap-1 flex-wrap">
                                                {row.nodes.map((node) => (
                                                    <StatePill key={node} bg="node">
                                                        {node}
                                                    </StatePill>
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

    return Array.from(byType.values()).sort((a, b) => a.label.localeCompare(b.label));
}

import React from "react";
import Card from "react-bootstrap/Card";
import Table from "react-bootstrap/Table";
import Spinner from "react-bootstrap/Spinner";
import { useAsync } from "react-async-hook";
import { useServices } from "hooks/useServices";
import { EmptySet } from "components/common/EmptySet";
import { StatePill } from "components/common/StatePill";
import NodeTagPill from "./NodeTagPill";
import { SortableHeader, useSortableData } from "./sortableTable";

type OngoingTask = Raven.Client.Documents.Operations.OngoingTasks.OngoingTask;
type OngoingTaskType = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskType;

interface DatabaseOngoingTasksProps {
    packageId: string;
    database: string;
    nodes: string[];
}

const taskTypeLabels: Partial<Record<OngoingTaskType, string>> = {
    Replication: "External Replication",
    RavenEtl: "RavenDB ETL",
    SqlEtl: "SQL ETL",
    OlapEtl: "OLAP ETL",
    ElasticSearchEtl: "Elasticsearch ETL",
    QueueEtl: "Queue ETL",
    QueueSink: "Queue Sink",
    Backup: "Backup",
    Subscription: "Subscription",
    PullReplicationAsHub: "Replication Hub",
    PullReplicationAsSink: "Replication Sink",
    SnowflakeEtl: "Snowflake ETL",
    EmbeddingsGeneration: "Embeddings Generation",
    GenAi: "GenAI",
};

function taskTypeLabel(type: OngoingTaskType): string {
    return taskTypeLabels[type] ?? type;
}

const sortAccessors: Record<string, (task: OngoingTask) => number | string> = {
    type: (task) => taskTypeLabel(task.TaskType),
    name: (task) => task.TaskName ?? "",
    responsibleNode: (task) => task.ResponsibleNode?.NodeTag ?? "",
    state: (task) => task.TaskState ?? "",
};

// Ongoing tasks are a cluster-wide concern - each node's tasks.json reports the same list with the
// runtime responsible node - so merge every node's entry by TaskId and show one table, surfacing which
// node actually runs each task (the gap the per-node count in the overview leaves open).
export default function DatabaseOngoingTasks({ packageId, database, nodes }: DatabaseOngoingTasksProps) {
    const { manageServerService } = useServices();

    const tasksAsync = useAsync(async () => {
        const settled = await Promise.allSettled(
            nodes.map((tag) => manageServerService.getDebugPackageDatabaseOngoingTasks(packageId, tag, database))
        );
        const byId = new Map<number, OngoingTask>();
        settled.forEach((outcome) => {
            if (outcome.status === "fulfilled" && outcome.value) {
                (outcome.value.OngoingTasks ?? []).forEach((task) => {
                    if (!byId.has(task.TaskId)) {
                        byId.set(task.TaskId, task);
                    }
                });
            }
        });
        return Array.from(byId.values());
    }, [packageId, database, nodes]);

    const tasks = tasksAsync.result ?? [];
    const { sorted, sortKey, sortDirection, requestSort } = useSortableData(
        tasks,
        sortAccessors,
        "responsibleNode",
        "asc"
    );
    const sortProps = { sortKey, sortDirection, onSort: requestSort };

    return (
        <div className="database-ongoing-tasks">
            <h3 className="mb-3">Ongoing Tasks</h3>
            <Card>
                <Card.Body className="vstack gap-3">
                    {tasksAsync.loading ? (
                        <div className="hstack gap-2 justify-content-center text-muted py-3">
                            <Spinner size="sm" /> Loading ongoing tasks...
                        </div>
                    ) : tasks.length === 0 ? (
                        <EmptySet compact>No ongoing tasks for {database} in the package</EmptySet>
                    ) : (
                        <>
                            <div className="text-muted">
                                {tasks.length} ongoing task{tasks.length === 1 ? "" : "s"}
                            </div>
                            <Table responsive className="m-0 align-middle">
                                <thead>
                                    <tr>
                                        <SortableHeader label="Task" columnKey="type" {...sortProps} />
                                        <SortableHeader label="Name" columnKey="name" {...sortProps} />
                                        <SortableHeader
                                            label="Responsible node"
                                            columnKey="responsibleNode"
                                            {...sortProps}
                                        />
                                        <SortableHeader label="State" columnKey="state" {...sortProps} />
                                    </tr>
                                </thead>
                                <tbody>
                                    {sorted.map((task) => (
                                        <tr key={task.TaskId}>
                                            <td className="fw-bold">{taskTypeLabel(task.TaskType)}</td>
                                            <td>{task.TaskName}</td>
                                            <td>
                                                {task.ResponsibleNode?.NodeTag ? (
                                                    <span className="hstack gap-1 align-items-center">
                                                        <NodeTagPill tag={task.ResponsibleNode.NodeTag} />
                                                        {task.PinToMentorNode && (
                                                            <span className="small-label">(pinned)</span>
                                                        )}
                                                    </span>
                                                ) : (
                                                    <span className="text-muted">Not assigned</span>
                                                )}
                                            </td>
                                            <td>
                                                <TaskStatePill task={task} />
                                            </td>
                                        </tr>
                                    ))}
                                </tbody>
                            </Table>
                        </>
                    )}
                </Card.Body>
            </Card>
        </div>
    );
}

function TaskStatePill({ task }: { task: OngoingTask }) {
    if (task.Error) {
        return <StatePill bg="danger">Error</StatePill>;
    }
    switch (task.TaskState) {
        case "Enabled":
            return <StatePill bg="success">Enabled</StatePill>;
        case "Disabled":
            return <StatePill bg="warning">Disabled</StatePill>;
        case "PartiallyEnabled":
            return <StatePill bg="warning">Partially enabled</StatePill>;
        default:
            return <StatePill bg="secondary">{task.TaskState}</StatePill>;
    }
}

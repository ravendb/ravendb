import React, { memo, useMemo } from "react";
import Spinner from "react-bootstrap/Spinner";
import { Icon } from "components/common/Icon";
import { useAsync } from "react-async-hook";
import { useServices } from "hooks/useServices";
import { EmptySet } from "components/common/EmptySet";
import NodeTagPill from "./NodeTagPill";
import {
    ColumnDef,
    getCoreRowModel,
    getFilteredRowModel,
    getSortedRowModel,
    useReactTable,
} from "@tanstack/react-table";
import VirtualTable from "components/common/virtualTable/VirtualTable";
import { virtualTableUtils } from "components/common/virtualTable/utils/virtualTableUtils";
import { analyzerConstants } from "./analyzerConstants";
import SummaryBar from "./SummaryBar";
import SizeGetter from "components/common/SizeGetter";

type OngoingTask = Raven.Client.Documents.Operations.OngoingTasks.OngoingTask;
type OngoingTaskType = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskType;

const EMPTY_TASKS: OngoingTask[] = [];

interface DatabaseOngoingTasksProps {
    packageId: string;
    database: string;
    nodes: string[];
    selectedNode: string;
}

interface DatabaseOngoingTasksWithSizeProps extends DatabaseOngoingTasksProps {
    width: number;
}

function useDatabaseOngoingTasksColumns(availableWidth: number) {
    const bodyWidth = virtualTableUtils.getTableBodyWidth(availableWidth);

    const taskColumns: ColumnDef<OngoingTask>[] = useMemo(() => {
        const getSize = virtualTableUtils.getCellSizeProvider(bodyWidth);
        return [
            {
                header: "Task",
                id: "type",
                accessorFn: (task) => taskTypeLabel(task.TaskType),
                cell: ({ getValue }) => <span className="fw-bold">{getValue<string>()}</span>,
                size: getSize(25),
            },
            {
                header: "Name",
                accessorKey: "TaskName",
                cell: taskNameCell,
                size: getSize(33),
            },
            {
                header: "Responsible node",
                id: "responsibleNode",
                accessorFn: (task) => task.ResponsibleNode?.NodeTag ?? "",
                cell: taskResponsibleNodeCell,
                size: getSize(20),
            },
            {
                header: "State",
                id: "state",
                accessorFn: (task) => task.TaskState ?? "",
                cell: ({ row }) => <TaskStateBadge task={row.original} />,
                size: getSize(22),
            },
        ];
    }, [bodyWidth]);

    return { taskColumns };
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

// Each node reports the full task list for the database - fetch from all nodes and merge by TaskId
// so we get the most complete picture even if one node's data is missing from the package.
// Then filter the displayed rows to tasks running on the selected node.
export default memo(function DatabaseOngoingTasks({
    packageId,
    database,
    nodes,
    selectedNode,
}: DatabaseOngoingTasksProps) {
    return (
        <SizeGetter
            render={({ width }) => (
                <DatabaseOngoingTasksWithSize
                    packageId={packageId}
                    database={database}
                    nodes={nodes}
                    selectedNode={selectedNode}
                    width={width}
                />
            )}
        />
    );
});

function DatabaseOngoingTasksWithSize({
    packageId,
    database,
    nodes,
    selectedNode,
    width,
}: DatabaseOngoingTasksWithSizeProps) {
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

    const allTasks = tasksAsync.result ?? EMPTY_TASKS;
    const tasks = useMemo(
        () => (selectedNode ? allTasks.filter((t) => t.ResponsibleNode?.NodeTag === selectedNode) : allTasks),
        [allTasks, selectedNode]
    );

    const enabledCount = tasks.filter((t) => !t.Error && t.TaskState === "Enabled").length;
    const disabledCount = tasks.filter((t) => !t.Error && t.TaskState === "Disabled").length;

    const { taskColumns } = useDatabaseOngoingTasksColumns(width);

    const table = useReactTable({
        data: tasks,
        columns: taskColumns,
        enableSorting: tasks.length > analyzerConstants.minRowsForControls,
        enableColumnFilters: tasks.length > analyzerConstants.minRowsForControls,
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
        getRowId: (row) => String(row.TaskId),
    });

    const heightInPx = virtualTableUtils.getHeightInPx(tasks.length, 400);

    const emptyMessage =
        allTasks.length === 0
            ? `No ongoing tasks for ${database} in the package`
            : `No ongoing tasks running on node ${selectedNode} for ${database}`;

    return (
        <div className="database-ongoing-tasks">
            <div className="panel-bg-1 rounded">
                <div className="p-4 vstack gap-3">
                    <h3 className="mb-0">Ongoing Tasks</h3>
                    {tasksAsync.loading ? (
                        <div className="hstack gap-2 justify-content-center text-muted py-3">
                            <Spinner size="sm" /> Loading ongoing tasks...
                        </div>
                    ) : tasks.length === 0 ? (
                        <EmptySet compact className="justify-content-center">
                            {emptyMessage}
                        </EmptySet>
                    ) : (
                        <>
                            <SummaryBar
                                items={[
                                    {
                                        icon: "ongoing-tasks",
                                        count: tasks.length,
                                        label: selectedNode ? `on node ${selectedNode}` : "total",
                                    },
                                    {
                                        icon: "ongoing-tasks",
                                        iconAddon: "check",
                                        count: enabledCount,
                                        label: "active",
                                        colorClass: "text-success",
                                    },
                                    {
                                        icon: "ongoing-tasks",
                                        iconAddon: "cancel",
                                        count: disabledCount,
                                        label: "disabled",
                                        colorClass: "text-warning",
                                    },
                                ]}
                            />
                            <VirtualTable table={table} heightInPx={heightInPx} />
                        </>
                    )}
                </div>
            </div>
        </div>
    );
}

function taskNameCell({ getValue }: { getValue: () => unknown }) {
    const v = getValue() as string;
    return <span title={v}>{v}</span>;
}

function taskResponsibleNodeCell({ row }: { row: { original: OngoingTask } }) {
    return row.original.ResponsibleNode?.NodeTag ? (
        <span className="hstack gap-1 align-items-center">
            <NodeTagPill tag={row.original.ResponsibleNode.NodeTag} />
            {row.original.PinToMentorNode && <span className="small-label">(pinned)</span>}
        </span>
    ) : (
        <span className="text-muted">Not assigned</span>
    );
}

function TaskStateBadge({ task }: { task: OngoingTask }) {
    if (task.Error) {
        return (
            <span className="hstack gap-1 text-danger">
                <Icon icon="danger" margin="m-0" /> Error
            </span>
        );
    }
    switch (task.TaskState) {
        case "Enabled":
            return (
                <span className="hstack gap-1 text-success">
                    <Icon icon="check" margin="m-0" /> Active
                </span>
            );
        case "Disabled":
            return (
                <span className="hstack gap-1 text-warning">
                    <Icon icon="cancel" margin="m-0" /> Disabled
                </span>
            );
        case "PartiallyEnabled":
            return (
                <span className="hstack gap-1 text-warning">
                    <Icon icon="warning" margin="m-0" /> Partially enabled
                </span>
            );
        default:
            return <span className="text-muted">{task.TaskState}</span>;
    }
}

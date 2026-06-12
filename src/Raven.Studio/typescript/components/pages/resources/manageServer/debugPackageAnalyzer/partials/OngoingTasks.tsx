import React, { useMemo } from "react";
import {
    ColumnDef,
    getCoreRowModel,
    getFilteredRowModel,
    getSortedRowModel,
    useReactTable,
} from "@tanstack/react-table";
import NodeTagPill from "./NodeTagPill";
import { EmptySet } from "components/common/EmptySet";
import VirtualTable from "components/common/virtualTable/VirtualTable";
import { virtualTableUtils } from "components/common/virtualTable/utils/virtualTableUtils";
import { analyzerConstants } from "./analyzerConstants";
import SummaryBar from "./SummaryBar";
import SizeGetter from "components/common/SizeGetter";

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

interface TaskTableRow {
    rowKind: "task" | "node";
    key: string;
    label?: string;
    nodeTag?: string;
    count: number;
}

interface OngoingTasksProps {
    summary: DebugPackageAnalysisSummary;
    nodeTag?: string;
}

interface OngoingTasksWithSizeProps extends OngoingTasksProps {
    width: number;
}

function useOngoingTasksColumns(availableWidth: number) {
    const bodyWidth = virtualTableUtils.getTableBodyWidth(availableWidth);
    const getSize = virtualTableUtils.getCellSizeProvider(bodyWidth);

    const taskColumns: ColumnDef<TaskTableRow>[] = useMemo(
        () => [
            {
                header: "Task",
                accessorKey: "label",
                cell: ongoingTaskLabelCell,
                size: getSize(57),
            },
            {
                header: "Node",
                accessorKey: "nodeTag",
                cell: ongoingTaskNodeTagCell,
                size: getSize(21),
            },
            {
                header: "Count",
                accessorKey: "count",
                cell: ({ getValue }) => getValue<number>().toLocaleString(),
                size: getSize(22),
            },
        ],
        [getSize]
    );

    return { taskColumns };
}

export default function OngoingTasks({ summary, nodeTag }: OngoingTasksProps) {
    return (
        <SizeGetter
            render={({ width }) => <OngoingTasksWithSize summary={summary} nodeTag={nodeTag} width={width} />}
        />
    );
}

function OngoingTasksWithSize({ summary, nodeTag, width }: OngoingTasksWithSizeProps) {
    const rows = useMemo(() => buildTaskRows(summary, nodeTag), [summary, nodeTag]);
    const taskRows = rows.filter((r) => r.rowKind === "task");
    const total = taskRows.reduce((sum, r) => sum + r.count, 0);

    const { taskColumns } = useOngoingTasksColumns(width);

    const table = useReactTable({
        data: rows,
        columns: taskColumns,
        enableSorting: rows.length > analyzerConstants.minRowsForControls,
        enableColumnFilters: rows.length > analyzerConstants.minRowsForControls,
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
        getRowId: (row) => row.key,
    });

    const heightInPx = virtualTableUtils.getHeightInPx(rows.length, 500);

    return (
        <div className="ongoing-tasks">
            <div className="panel-bg-1 rounded">
                <div className="p-4 vstack gap-3">
                    <h3 className="mb-0">Ongoing Tasks</h3>
                    <SummaryBar items={[{ icon: "ongoing-tasks", count: total, label: "total" }]} />
                    {taskRows.length === 0 ? (
                        <EmptySet compact className="justify-content-center">
                            No ongoing tasks in the package
                        </EmptySet>
                    ) : (
                        <VirtualTable table={table} heightInPx={heightInPx} />
                    )}
                </div>
            </div>
        </div>
    );
}

function ongoingTaskLabelCell({ row }: { row: { original: TaskTableRow } }) {
    return row.original.rowKind === "task" ? <span className="fw-bold">{row.original.label}</span> : null;
}

function ongoingTaskNodeTagCell({ row }: { row: { original: TaskTableRow } }) {
    return row.original.rowKind === "node" ? <NodeTagPill tag={row.original.nodeTag!} /> : null;
}

function buildTaskRows(summary: DebugPackageAnalysisSummary, nodeTag?: string): TaskTableRow[] {
    const byType = new Map<
        string,
        {
            totalCount: number;
            nodes: { nodeTag: string; count: number }[];
        }
    >();

    Object.entries(summary.SummaryPerNode ?? {}).forEach(([tag, node]) => {
        if (nodeTag && tag !== nodeTag) return;

        (node.DatabasesOngoingTasks?.Items ?? []).forEach((item) => {
            taskTypeLabels.forEach(({ field, label }) => {
                const count = item[field] as number;
                if (count > 0) {
                    let agg = byType.get(label);
                    if (!agg) {
                        agg = { totalCount: 0, nodes: [] };
                        byType.set(label, agg);
                    }
                    agg.totalCount += count;
                    const existing = agg.nodes.find((n) => n.nodeTag === tag);
                    if (existing) {
                        existing.count += count;
                    } else {
                        agg.nodes.push({ nodeTag: tag, count });
                    }
                }
            });
        });
    });

    const result: TaskTableRow[] = [];

    [...byType.keys()]
        .sort((a, b) => a.localeCompare(b))
        .forEach((label) => {
            const agg = byType.get(label)!;

            result.push({
                rowKind: "task",
                key: label,
                label,
                count: agg.totalCount,
            });

            agg.nodes
                .sort((a, b) => a.nodeTag.localeCompare(b.nodeTag))
                .forEach(({ nodeTag: tag, count }) => {
                    result.push({
                        rowKind: "node",
                        key: `${label}/${tag}`,
                        nodeTag: tag,
                        count,
                    });
                });
        });

    return result;
}

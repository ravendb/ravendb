import React, { useMemo, useState } from "react";
import {
    ColumnDef,
    ExpandedState,
    Row,
    getCoreRowModel,
    getExpandedRowModel,
    getFilteredRowModel,
    getSortedRowModel,
    useReactTable,
} from "@tanstack/react-table";
import NodeTagPill from "./NodeTagPill";
import { ExpandIndicator, NodeTagPillStack, expandableRowProps } from "./nodeStackTable";
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
    nodeTags?: string[];
    subRows?: TaskTableRow[];
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
                cell: OngoingTaskLabelCell,
                size: getSize(57),
            },
            {
                header: "Node",
                accessorKey: "nodeTag",
                cell: OngoingTaskNodeTagCell,
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
    const [expanded, setExpanded] = useState<ExpandedState>({});
    const total = rows.reduce((sum, r) => sum + r.count, 0);

    const { taskColumns } = useOngoingTasksColumns(width);

    const table = useReactTable({
        data: rows,
        columns: taskColumns,
        state: { expanded },
        onExpandedChange: setExpanded,
        getSubRows: (row) => row.subRows,
        getRowCanExpand: (row) => (row.original.subRows?.length ?? 0) > 0,
        enableSorting: rows.length > analyzerConstants.minRowsForControls,
        enableColumnFilters: rows.length > analyzerConstants.minRowsForControls,
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
        getExpandedRowModel: getExpandedRowModel(),
        getRowId: (row) => row.key,
    });

    const heightInPx = virtualTableUtils.getHeightInPx(table.getRowModel().rows.length, 500);

    return (
        <div className="ongoing-tasks">
            <div className="panel-bg-1 rounded">
                <div className="p-4 vstack gap-3">
                    <h3 className="mb-0">Ongoing Tasks</h3>
                    <SummaryBar items={[{ icon: "ongoing-tasks", count: total, label: "total" }]} />
                    {rows.length === 0 ? (
                        <EmptySet compact className="justify-content-center">
                            No ongoing tasks in the package
                        </EmptySet>
                    ) : (
                        <VirtualTable table={table} heightInPx={heightInPx} {...expandableRowProps<TaskTableRow>()} />
                    )}
                </div>
            </div>
        </div>
    );
}

function OngoingTaskLabelCell({ row }: { row: Row<TaskTableRow> }) {
    if (row.original.rowKind !== "task") {
        return null;
    }
    return (
        <span className="hstack gap-1 fw-bold">
            {row.getCanExpand() && <ExpandIndicator expanded={row.getIsExpanded()} />}
            {row.original.label}
        </span>
    );
}

function OngoingTaskNodeTagCell({ row }: { row: Row<TaskTableRow> }) {
    if (row.original.rowKind === "node") {
        return <NodeTagPill tag={row.original.nodeTag!} />;
    }
    const tags = row.original.nodeTags ?? [];
    return tags.length > 0 ? <NodeTagPillStack tags={tags} /> : null;
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
        if (nodeTag && tag !== nodeTag) {
            return;
        }

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

            const sortedNodes = [...agg.nodes].sort((a, b) => a.nodeTag.localeCompare(b.nodeTag));

            result.push({
                rowKind: "task",
                key: label,
                label,
                count: agg.totalCount,
                nodeTags: sortedNodes.map((n) => n.nodeTag),
                subRows: sortedNodes.map(
                    ({ nodeTag: tag, count }): TaskTableRow => ({
                        rowKind: "node",
                        key: `${label}/${tag}`,
                        nodeTag: tag,
                        count,
                    })
                ),
            });
        });

    return result;
}

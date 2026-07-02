import {
    AnyEtlOngoingTaskInfo,
    OngoingEtlTaskNodeInfo,
    OngoingTaskInfo,
    OngoingTaskSharedInfo,
} from "components/models/tasks";
import { databaseLocationComparator } from "components/utils/common";
import IconName from "typings/server/icons";
import assertUnreachable from "components/utils/assertUnreachable";
import { useAppSelector } from "components/store";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppUrls } from "hooks/useAppUrls";
import { BaseOngoingTaskPanelProps, useTasksOperations } from "../../shared/shared";
import EtlTaskStats = Raven.Server.Documents.ETL.Stats.EtlTaskStats;
import TaskErrors = Raven.Server.Documents.TasksErrors.TaskErrors;
import {
    TaskErrorsWithLocation,
    getTaskHealthStatus,
    healthStatusToBadge,
    parseProcessName,
} from "components/pages/database/tasks/tasksErrors/utils/tasksErrorsUtils";

export type EtlHealthStatus = Raven.Server.Documents.TasksErrors.OngoingTaskHealthStatus;

const etlTaskTypeLabels: Partial<Record<StudioTaskType, string>> = {
    RavenEtl: "RavenDB ETL",
    SqlEtl: "SQL ETL",
    OlapEtl: "OLAP ETL",
    ElasticSearchEtl: "Elasticsearch ETL",
    KafkaQueueEtl: "Kafka ETL",
    RabbitQueueEtl: "RabbitMQ ETL",
    AzureQueueStorageQueueEtl: "Azure Queue Storage ETL",
    AmazonSqsQueueEtl: "Amazon SQS ETL",
    SnowflakeEtl: "Snowflake ETL",
    EmbeddingsGeneration: "Embeddings Generation",
    GenAi: "GenAI",
};

export function getEtlTaskTypeLabel(taskType: StudioTaskType): string {
    return etlTaskTypeLabels[taskType] ?? taskType;
}

const etlTaskTypeIcons: Partial<Record<StudioTaskType, IconName>> = {
    RavenEtl: "ravendb-etl",
    SqlEtl: "sql-etl",
    OlapEtl: "olap-etl",
    ElasticSearchEtl: "elastic-search-etl",
    KafkaQueueEtl: "kafka-etl",
    RabbitQueueEtl: "rabbitmq-etl",
    AzureQueueStorageQueueEtl: "azure-queue-storage-etl",
    AmazonSqsQueueEtl: "amazon-sqs-etl",
    SnowflakeEtl: "snowflake-etl",
    EmbeddingsGeneration: "ai-etl",
    GenAi: "genai",
};

export function getEtlTaskTypeIcon(taskType: StudioTaskType): IconName {
    return etlTaskTypeIcons[taskType] ?? "etl";
}

export function getPopoverMessageForTaskHealth(status: EtlHealthStatus): string {
    switch (status) {
        case "Healthy":
            return "This task is in good health, with no errors or only a low error rate.";
        case "Impaired":
            return "This task needs your attention because it has an increased error rate.";
        case "Failed":
            return "This task needs your attention because it has a high error rate.";
        default:
            return assertUnreachable(status);
    }
}

export interface EtlPanelProgress {
    state: "success" | "running";
    icon?: IconName;
    progress?: number;
    label: string;
}

export function getTaskErrorCount(taskErrors: TaskErrors[], taskName: string): number {
    return filterTaskErrors(taskErrors, taskName).reduce(
        (acc, e) => acc + e.ProcessErrors.length + e.ItemErrors.length,
        0
    );
}

export interface TaskErrorsByLocation extends databaseLocationSpecifier {
    errorCount: number;
}

export function getTaskErrorCountByLocation(
    taskErrors: TaskErrorsWithLocation[],
    taskName: string,
    locations: databaseLocationSpecifier[]
): TaskErrorsByLocation[] {
    const locationKey = (l: databaseLocationSpecifier) => `${l.nodeTag}/${l.shardNumber ?? ""}`;
    const counts = new Map<string, TaskErrorsByLocation>(
        locations.map((l) => [locationKey(l), { nodeTag: l.nodeTag, shardNumber: l.shardNumber, errorCount: 0 }])
    );

    for (const e of filterTaskErrors(taskErrors, taskName)) {
        const count = e.ProcessErrors.length + e.ItemErrors.length;
        if (count === 0) {
            continue;
        }

        const existing = counts.get(locationKey(e));
        if (existing) {
            existing.errorCount += count;
        } else {
            counts.set(locationKey(e), { nodeTag: e.nodeTag, shardNumber: e.shardNumber, errorCount: count });
        }
    }

    return [...counts.values()];
}

function filterTaskErrors<T extends TaskErrors>(taskErrors: T[], taskName: string): T[] {
    // Reuse the shared split rule so the panel badge and the Tasks Errors page stay in sync: ETL/AI
    // names are "taskName/transformationName", CDC names are matched whole.
    return taskErrors.filter((e) => parseProcessName(e.TaskName, e.Category)[0] === taskName);
}

export function computeEtlPanelProgress(
    data: OngoingTaskInfo<OngoingTaskSharedInfo, OngoingEtlTaskNodeInfo>
): EtlPanelProgress {
    const disabled = data.shared.taskState === "Disabled";

    const responsibleNodeInfos = data.nodesInfo.filter(
        (nodeInfo) =>
            nodeInfo.details && data.responsibleLocations.some((l) => databaseLocationComparator(l, nodeInfo.location))
    );

    const allProgress = responsibleNodeInfos.flatMap((nodeInfo) => nodeInfo.etlProgress ?? []);

    if (allProgress.length === 0) {
        return { state: "running", icon: disabled ? "stop" : null, label: disabled ? "Disabled" : "?" };
    }

    if (allProgress.every((x) => x.completed) && data.shared.taskState === "Enabled") {
        return { state: "success", icon: "check", label: "up to date" };
    }

    const totalItems = allProgress.reduce((acc, p) => acc + p.global.total, 0);
    const totalProcessed = allProgress.reduce((acc, p) => acc + p.global.processed, 0);
    const percentage = totalItems === 0 ? 1 : Math.floor((totalProcessed * 100) / totalItems) / 100;
    const anyDisabled = allProgress.some((x) => x.disabled);

    return {
        state: "running",
        icon: anyDisabled ? "stop" : null,
        progress: percentage,
        label: anyDisabled ? "Disabled" : "Running",
    };
}

export type EtlPanelBaseProps<T extends AnyEtlOngoingTaskInfo> = BaseOngoingTaskPanelProps<T> & {
    etlStats?: EtlTaskStats[];
    taskErrors?: TaskErrorsWithLocation[];
};

export function useEtlPanel<T extends AnyEtlOngoingTaskInfo>(props: EtlPanelBaseProps<T>, editUrl: string) {
    const { data, etlStats, taskErrors } = props;

    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();
    const { appUrl } = useAppUrls();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    const canEdit = hasDatabaseAdminAccess && !data.shared.serverWide;
    const goToTaskErrors = appUrl.forTasksErrors(databaseName, { taskName: data.shared.taskName });

    const { detailsVisible, toggleDetails, onEdit } = useTasksOperations(editUrl, props);

    const taskHealth = getTaskHealthStatus(etlStats ?? [], data.shared.taskName);
    const healthBadge = healthStatusToBadge(taskHealth);
    const errorCount = getTaskErrorCount(taskErrors ?? [], data.shared.taskName);
    const errorsByLocation = getTaskErrorCountByLocation(
        taskErrors ?? [],
        data.shared.taskName,
        data.responsibleLocations
    );
    const etlProgress = computeEtlPanelProgress(data);

    return {
        canEdit,
        goToTaskErrors,
        detailsVisible,
        toggleDetails,
        onEdit,
        taskHealth,
        healthBadge,
        errorCount,
        errorsByLocation,
        etlProgress,
    };
}

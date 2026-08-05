import IconName from "typings/server/icons";
import { RavenBadgeBgVariants } from "react-bootstrap/Badge";
import appUrl from "common/appUrl";
import assertUnreachable from "components/utils/assertUnreachable";
import TaskUtils from "components/utils/TaskUtils";
import EtlTaskStats = Raven.Server.Documents.ETL.Stats.EtlTaskStats;
import TaskErrors = Raven.Server.Documents.TasksErrors.TaskErrors;
import { ThemeColor } from "components/models/common";

export type TaskErrorStep = Raven.Server.Documents.TasksErrors.TaskErrorStep;
export type EtlHealthStatus = Raven.Server.Documents.TasksErrors.OngoingTaskHealthStatus;

export type GroupByType = "task" | "none";

export function getEtlEditLink(databaseName: string, taskId: number, etlType: StudioEtlType): string | null {
    if (taskId == null || etlType == null) {
        return null;
    }

    switch (etlType) {
        case "Raven":
            return appUrl.forEditRavenEtl(databaseName, taskId);
        case "Sql":
            return appUrl.forEditSqlEtl(databaseName, taskId);
        case "Olap":
            return appUrl.forEditOlapEtl(databaseName, taskId);
        case "ElasticSearch":
            return appUrl.forEditElasticSearchEtl(databaseName, taskId);
        case "Kafka":
            return appUrl.forEditKafkaEtl(databaseName, taskId);
        case "RabbitMQ":
            return appUrl.forEditRabbitMqEtl(databaseName, taskId);
        case "AzureQueueStorage":
            return appUrl.forEditAzureQueueStorageEtl(databaseName, taskId);
        case "AmazonSqs":
            return appUrl.forEditAmazonSqsEtl(databaseName, taskId);
        case "Snowflake":
            return appUrl.forEditSnowflakeEtl(databaseName, taskId);
        case "EmbeddingsGeneration":
            return appUrl.forEditEmbeddingsGeneration(databaseName, taskId);
        case "GenAi":
            return appUrl.forEditGenAi(databaseName, taskId);
        default:
            return assertUnreachable(etlType);
    }
}

export type TaskErrorsWithLocation = TaskErrors & databaseLocationSpecifier;

export interface TransformationWithErrors {
    transformationName: string;
    processErrors: (TaskErrors["ProcessErrors"][number] & databaseLocationSpecifier)[];
    itemErrors: (TaskErrors["ItemErrors"][number] & databaseLocationSpecifier)[];
}

export interface TaskWithErrors {
    etlName: string;
    etlType?: StudioEtlType;
    category: TaskCategory;
    transformations: TransformationWithErrors[];
}

export interface TaskError {
    etlName: string;
    transformationName: string;
    healthStatus: EtlHealthStatus;
    taskId?: number;
    etlType?: StudioEtlType;
    category: TaskCategory;
}

export type FlatError = (
    | (TransformationWithErrors["itemErrors"][number] & { errorType: "Item" })
    | (TransformationWithErrors["processErrors"][number] & { errorType: "Process" })
) &
    TaskError;

export interface TasksFiltersState {
    searchText: string;
    nodeTags: string[];
    shardNumbers: string[];
    healthStatuses: EtlHealthStatus[];
    taskTypes: StudioTaskType[];
}

// ETL and AI errors are stored as "taskName/transformationName". CDC task names carry no
// transformation and may themselves contain "/", so CDC names are never split.
export function parseProcessName(
    processName: string,
    category: TaskCategory
): [etlName: string, transformationName: string] {
    const slashIndex = processName.indexOf("/");
    if (!taskHasTransformations(category) || slashIndex === -1) {
        return [processName, ""];
    }
    return [processName.slice(0, slashIndex), processName.slice(slashIndex + 1)];
}

export function getTasksWithErrors(processes: TaskErrorsWithLocation[], etlStats: EtlTaskStats[]): TaskWithErrors[] {
    if (!processes?.length) {
        return [];
    }

    return _.chain(processes)
        .filter((p: TaskErrorsWithLocation) => _.size(p?.ProcessErrors) || _.size(p?.ItemErrors))
        .groupBy((p: TaskErrorsWithLocation) => parseProcessName(p.TaskName, p.Category)[0])
        .map(
            (group: TaskErrorsWithLocation[], etlName: string): TaskWithErrors => ({
                etlName,
                etlType: resolveEtlType(etlStats, etlName),
                category: group[0].Category,
                transformations: _.chain(group)
                    .groupBy((p: TaskErrorsWithLocation) => parseProcessName(p.TaskName, p.Category)[1])
                    .map(
                        (
                            transformationGroup: TaskErrorsWithLocation[],
                            transformationName: string
                        ): TransformationWithErrors => ({
                            transformationName,
                            processErrors: transformationGroup.flatMap((p) =>
                                p.ProcessErrors.map((e) => ({ ...e, nodeTag: p.nodeTag, shardNumber: p.shardNumber }))
                            ),
                            itemErrors: transformationGroup.flatMap((p) =>
                                p.ItemErrors.map((e) => ({ ...e, nodeTag: p.nodeTag, shardNumber: p.shardNumber }))
                            ),
                        })
                    )
                    .value(),
            })
        )
        .value();
}

export function flattenTransformationErrors(
    itemErrors: TransformationWithErrors["itemErrors"],
    processErrors: TransformationWithErrors["processErrors"]
) {
    return [
        ...itemErrors.map((e) => ({ ...e, errorType: "Item" as const, AffectedDocumentsCount: 1 })),
        ...processErrors.map((e) => ({ ...e, errorType: "Process" as const })),
    ];
}

export function flattenAllTasksErrors(tasksWithErrors: TaskWithErrors[], etlStats: EtlTaskStats[]): FlatError[] {
    return tasksWithErrors.flatMap((task) => {
        const taskStats = etlStats.find((s) => s.TaskName === task.etlName);
        const taskId = taskStats?.TaskId;
        const etlType = task.etlType;
        const category = task.category;

        return task.transformations.flatMap((transformation) => {
            const healthStatus =
                taskStats?.Stats.find((s) => s.TransformationName === transformation.transformationName)?.Statistics
                    .HealthStatus ?? null;

            return [
                ...transformation.itemErrors.map((e) => ({
                    ...e,
                    errorType: "Item" as const,
                    AffectedDocumentsCount: 1,
                    etlName: task.etlName,
                    transformationName: transformation.transformationName,
                    healthStatus,
                    taskId,
                    etlType,
                    category,
                })),
                ...transformation.processErrors.map((e) => ({
                    ...e,
                    errorType: "Process" as const,
                    etlName: task.etlName,
                    transformationName: transformation.transformationName,
                    healthStatus,
                    taskId,
                    etlType,
                    category,
                })),
            ];
        });
    });
}

export function getHealthStatusFromStats(stats: EtlTaskStats["Stats"]): EtlHealthStatus {
    if (stats.some((s) => s.Statistics.HealthStatus === "Failed")) {
        return "Failed";
    }

    if (stats.some((s) => s.Statistics.HealthStatus === "Impaired")) {
        return "Impaired";
    }

    return "Healthy";
}

export function getTaskHealthStatus(etlStats: EtlTaskStats[], etlName: string): EtlHealthStatus {
    const stats = etlStats.filter((s) => s.TaskName === etlName).flatMap((s) => s.Stats);
    return getHealthStatusFromStats(stats);
}

export function getTaskPillColor(stats: EtlTaskStats["Stats"]): `bg-${ThemeColor}` {
    const health = getHealthStatusFromStats(stats);
    if (health === "Failed") {
        return "bg-danger";
    }

    if (health === "Impaired") {
        return "bg-warning";
    }

    return "bg-success";
}

interface HealthStatusBadge {
    bg: RavenBadgeBgVariants;
    icon: IconName;
    label: EtlHealthStatus | "Unknown";
}
export function healthStatusToBadge(status?: EtlHealthStatus): HealthStatusBadge {
    switch (status) {
        case "Failed":
            return { bg: "danger", icon: "close", label: "Failed" };
        case "Impaired":
            return { bg: "warning", icon: "warning", label: "Impaired" };
        case "Healthy":
            return { bg: "success", icon: "check", label: "Healthy" };
        default:
            return { bg: "secondary", icon: "help", label: "Unknown" };
    }
}

export function getStepIcon(step: TaskErrorStep): IconName {
    switch (step) {
        case "Transformation":
            return "replace";
        case "Load":
            return "import";
        case "Configuration":
            return "config";
        case "Extraction":
            return "export";
        case "ModelInference":
            return "ai";
        case "Persistence":
            return "save";
        case "Unknown":
            return "help";
        default:
            return assertUnreachable(step);
    }
}

export function getTaskTypeDisplay(
    category: TaskCategory,
    etlType: StudioEtlType | undefined
): { icon: IconName; label: string } {
    return TaskUtils.studioTaskTypeToDisplay(resolveStudioTaskType(category, etlType));
}

export function resolveStudioTaskType(
    category: TaskCategory,
    etlType: StudioEtlType | undefined
): StudioTaskType | undefined {
    switch (category) {
        case "Etl":
        case "Ai":
            return etlType ? TaskUtils.studioEtlTypeToStudioTaskType(etlType) : undefined;
        case "CdcSink":
            return "CdcSink";
        default:
            return assertUnreachable(category);
    }
}

export function getPopoverMessageForErrorType(errorType: "Item" | "Process"): string {
    switch (errorType) {
        case "Item":
            return "An error that occurred while processing a single document. The document was skipped, and the task continues processing the remaining documents.";
        case "Process":
            return "An error that occurred at the batch level, potentially affecting multiple documents in the batch.";
        default:
            return assertUnreachable(errorType);
    }
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

export const SHOW_WIDTH_SIZE = 70;

export const AI_ONLY_TASK_TYPES: StudioTaskType[] = ["EmbeddingsGeneration", "GenAi"];

export type TaskCategory = "Etl" | "Ai" | "CdcSink";

// ETL and AI tasks report errors per transformation and are stored as "taskName/transformationName"
// (AI tasks are ETL processes under the hood). CDC task names carry no transformation and may
// themselves contain "/", so they're shown by task name only.
export function taskHasTransformations(category: TaskCategory): boolean {
    return category === "Etl" || category === "Ai";
}

function resolveEtlType(etlStats: EtlTaskStats[], etlName: string): StudioEtlType | undefined {
    const stats = etlStats.find((s) => s.TaskName === etlName);
    return TaskUtils.etlTypeToStudioType(stats?.EtlType, stats?.EtlSubType) ?? undefined;
}

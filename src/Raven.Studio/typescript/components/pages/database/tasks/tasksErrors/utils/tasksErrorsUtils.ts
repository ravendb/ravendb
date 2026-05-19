import IconName from "typings/server/icons";
import { RavenBadgeBgVariants } from "react-bootstrap/Badge";
import appUrl from "common/appUrl";
import assertUnreachable from "components/utils/assertUnreachable";
import TaskUtils from "components/utils/TaskUtils";
import EtlTaskStats = Raven.Server.Documents.ETL.Stats.EtlTaskStats;
import EtlErrors = Raven.Server.Documents.ETL.Stats.TaskErrors;
import { ThemeColor } from "components/models/common";

export type EtlErrorStep = Raven.Server.Documents.ETL.TaskErrorStep;
export type EtlHealthStatus = Raven.Server.Documents.ETL.EtlProcessHealthStatus;

export type GroupByType = "task" | "none";

export type Direction = 1 | -1;

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

export type EtlErrorsWithLocation = EtlErrors & databaseLocationSpecifier;

export interface EtlTransformationWithErrors {
    transformationName: string;
    processErrors: (EtlErrors["ProcessErrors"][number] & databaseLocationSpecifier)[];
    itemErrors: (EtlErrors["ItemErrors"][number] & databaseLocationSpecifier)[];
}

export interface EtlTaskWithErrors {
    etlName: string;
    etlType?: StudioEtlType;
    transformations: EtlTransformationWithErrors[];
}

export interface EtlError {
    etlName: string;
    transformationName: string;
    healthStatus: EtlHealthStatus;
    taskId?: number;
    etlType?: StudioEtlType;
}

export type FlatError = (
    | (EtlTransformationWithErrors["itemErrors"][number] & { errorType: "Item" })
    | (EtlTransformationWithErrors["processErrors"][number] & { errorType: "Process" })
) &
    EtlError;

export interface TasksFiltersState {
    searchText: string;
    nodeTags: string[];
    shardNumbers: string[];
    healthStatuses: EtlHealthStatus[];
    taskTypes: StudioEtlType[];
}

export function parseProcessName(processName: string): [etlName: string, transformationName: string] {
    const slashIndex = processName.indexOf("/");
    if (slashIndex === -1) {
        return [processName, ""];
    }
    return [processName.slice(0, slashIndex), processName.slice(slashIndex + 1)];
}

export function getTasksWithErrors(processes: EtlErrorsWithLocation[], etlStats: EtlTaskStats[]): EtlTaskWithErrors[] {
    if (!processes?.length) {
        return [];
    }

    return _.chain(processes)
        .filter((p: EtlErrorsWithLocation) => _.size(p?.ProcessErrors) || _.size(p?.ItemErrors))
        .groupBy((p: EtlErrorsWithLocation) => parseProcessName(p.TaskName)[0])
        .map(
            (group: EtlErrorsWithLocation[], etlName: string): EtlTaskWithErrors => ({
                etlName,
                etlType: resolveEtlType(etlStats, etlName),
                transformations: _.chain(group)
                    .groupBy((p: EtlErrorsWithLocation) => parseProcessName(p.TaskName)[1])
                    .map(
                        (
                            transformationGroup: EtlErrorsWithLocation[],
                            transformationName: string
                        ): EtlTransformationWithErrors => ({
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
    itemErrors: EtlTransformationWithErrors["itemErrors"],
    processErrors: EtlTransformationWithErrors["processErrors"]
) {
    return [
        ...itemErrors.map((e) => ({ ...e, errorType: "Item" as const, AffectedDocumentsCount: 1 })),
        ...processErrors.map((e) => ({ ...e, errorType: "Process" as const })),
    ];
}

export function flattenAllTasksErrors(tasksWithErrors: EtlTaskWithErrors[], etlStats: EtlTaskStats[]): FlatError[] {
    return tasksWithErrors.flatMap((task) => {
        const taskStats = etlStats.find((s) => s.TaskName === task.etlName);
        const taskId = taskStats?.TaskId;
        const etlType = task.etlType;

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
                })),
                ...transformation.processErrors.map((e) => ({
                    ...e,
                    errorType: "Process" as const,
                    etlName: task.etlName,
                    transformationName: transformation.transformationName,
                    healthStatus,
                    taskId,
                    etlType,
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

export function getStepIcon(step: EtlErrorStep): IconName {
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

export function getEtlTypeIcon(value: StudioEtlType): IconName {
    switch (value) {
        case "Raven":
            return "ravendb-etl";
        case "Sql":
            return "sql-etl";
        case "Olap":
            return "olap-etl";
        case "ElasticSearch":
            return "elastic-search-etl";
        case "Kafka":
            return "kafka-etl";
        case "AzureQueueStorage":
            return "azure-queue-storage-etl";
        case "RabbitMQ":
            return "rabbitmq-etl";
        case "EmbeddingsGeneration":
            return "ai-etl";
        case "AmazonSqs":
            return "amazon-sqs-etl";
        case "Snowflake":
            return "snowflake-etl";
        case "GenAi":
            return "genai";
        default:
            return assertUnreachable(value);
    }
}

export function getEtlTypeLabel(etlType: StudioEtlType): string {
    switch (etlType) {
        case "Raven":
            return "RavenDB ETL";
        case "Sql":
            return "SQL ETL";
        case "Snowflake":
            return "Snowflake ETL";
        case "Olap":
            return "OLAP ETL";
        case "ElasticSearch":
            return "ElasticSearch ETL";
        case "Kafka":
            return "Kafka ETL";
        case "RabbitMQ":
            return "RabbitMQ ETL";
        case "AzureQueueStorage":
            return "Azure Queue Storage ETL";
        case "AmazonSqs":
            return "Amazon SQS ETL";
        case "EmbeddingsGeneration":
            return "Embeddings Generation";
        case "GenAi":
            return "GenAI";
        default:
            return assertUnreachable(etlType);
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

export const AI_ONLY_TASK_TYPES: StudioEtlType[] = ["EmbeddingsGeneration", "GenAi"];

export type TaskCategory = "Etl" | "Ai";

export function getTaskCategory(etlType: StudioEtlType | undefined): TaskCategory {
    return etlType && AI_ONLY_TASK_TYPES.includes(etlType) ? "Ai" : "Etl";
}

function resolveEtlType(etlStats: EtlTaskStats[], etlName: string): StudioEtlType | undefined {
    const stats = etlStats.find((s) => s.TaskName === etlName);
    return TaskUtils.etlTypeToStudioType(stats?.EtlType, stats?.EtlSubType) ?? undefined;
}

import EtlType = Raven.Client.Documents.Operations.ETL.EtlType;
import OngoingTaskType = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskType;
import OngoingTask = Raven.Client.Documents.Operations.OngoingTasks.OngoingTask;
import OngoingTaskQueueEtlDetails = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskQueueEtlDetails;
import OngoingTaskQueueEtlListView = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskQueueEtlListView;
import assertUnreachable from "./assertUnreachable";

export default class TaskUtils {
    static ongoingTaskToStudioTaskType(task: OngoingTask): StudioTaskType {
        if (task.TaskType === "QueueEtl") {
            const queueTask = task as OngoingTaskQueueEtlListView;
            switch (queueTask.BrokerType) {
                case "Kafka":
                    return "KafkaQueueEtl";
                case "RabbitMq":
                    return "RabbitQueueEtl";
                default:
                    assertUnreachable(queueTask.BrokerType);
            }
        }

        return task.TaskType;
    }

    static studioEtlTypeToEtlType(type: StudioEtlType): EtlType {
        switch (type) {
            case "Kafka":
            case "RabbitMQ":
                return "Queue";
            default:
                return type;
        }
    }

    static studioTaskTypeToTaskType(type: StudioTaskType): OngoingTaskType {
        if (type === "KafkaQueueEtl" || type === "RabbitQueueEtl") {
            return "QueueEtl";
        }

        return type;
    }

    static etlTypeToStudioType(etlType: EtlType, etlSubType: string): StudioEtlType {
        if (etlType === "Queue") {
            switch (etlSubType) {
                case "Kafka":
                    return "Kafka";
                case "RabbitMq":
                    return "RabbitMQ";
            }
        }

        return etlType as StudioEtlType;
    }

    static etlTypeToTaskType(etlType: EtlType): OngoingTaskType {
        switch (etlType) {
            case "ElasticSearch":
                return "ElasticSearchEtl";
            case "Olap":
                return "OlapEtl";
            case "Raven":
                return "RavenEtl";
            case "Sql":
                return "SqlEtl";
            case "Queue":
                return "QueueEtl";
            default:
                throw new Error("Unknown etl type mapping: " + etlType);
        }
    }

    static taskTypeToEtlType(taskType: OngoingTaskType): EtlType {
        switch (taskType) {
            case "RavenEtl":
                return "Raven";
            case "OlapEtl":
                return "Olap";
            case "ElasticSearchEtl":
                return "ElasticSearch";
            case "SqlEtl":
                return "Sql";
            case "QueueEtl":
                return "Queue";
            default:
                throw new Error("Unsupported task type: " + taskType);
        }
    }

    static formatStudioEtlType(etlType: StudioEtlType) {
        switch (etlType) {
            case "Raven":
                return "RavenDB";
            case "Sql":
                return "SQL";
            case "Olap":
                return "OLAP";
            case "ElasticSearch":
                return "Elasticsearch";
            case "Kafka":
                return "Kafka";
            case "RabbitMQ":
                return "RabbitMQ";
            default:
                assertUnreachable(etlType);
        }
    }
}

import EtlType = Raven.Client.Documents.Operations.ETL.EtlType;
import OngoingTaskType = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskType;
import OngoingTask = Raven.Client.Documents.Operations.OngoingTasks.OngoingTask;
import OngoingTaskQueueEtl = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskQueueEtl;
import assertUnreachable from "./assertUnreachable";
import OngoingTaskQueueSink = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskQueueSink;

export default class TaskUtils {
    static ongoingTaskToStudioTaskType(task: OngoingTask): StudioTaskType {
        if (task.TaskType === "QueueEtl") {
            const queueTask = task as OngoingTaskQueueEtl;
            switch (queueTask.BrokerType) {
                case "Kafka":
                    return "KafkaQueueEtl";
                case "RabbitMq":
                    return "RabbitQueueEtl";
                case "AzureQueueStorage":
                    return "AzureQueueStorageQueueEtl";
                case "None":
                    throw new Error("Expected non-null BrokerType");
                default:
                    assertUnreachable(queueTask.BrokerType);
            }
        }

        if (task.TaskType === "QueueSink") {
            const queueTask = task as OngoingTaskQueueSink;
            switch (queueTask.BrokerType) {
                case "Kafka":
                    return "KafkaQueueSink";
                case "RabbitMq":
                    return "RabbitQueueSink";
                case "AzureQueueStorage":
                    throw new Error("Not yet supported");
                case "None":
                    throw new Error("Expected non-null BrokerType");
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
            case "AzureQueueStorage":
                return "Queue";
            default:
                return type;
        }
    }

    static queueTypeToStudioType(
        brokerType: Raven.Client.Documents.Operations.ETL.Queue.QueueBrokerType
    ): StudioQueueSinkType {
        switch (brokerType) {
            case "Kafka":
                return "KafkaQueueSink";
            case "RabbitMq":
                return "RabbitQueueSink";
            case "AzureQueueStorage":
                throw new Error("Not yet supported");
            case "None":
                return null;
            default:
                assertUnreachable(brokerType);
        }
    }

    static studioTaskTypeToTaskType(type: StudioTaskType): OngoingTaskType {
        if (type === "KafkaQueueEtl" || type === "RabbitQueueEtl" || type === "AzureQueueStorageQueueEtl") {
            return "QueueEtl";
        }

        if (type === "KafkaQueueSink" || type === "RabbitQueueSink") {
            return "QueueSink";
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
                case "AzureQueueStorage":
                    return "AzureQueueStorage";
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
            case "Snowflake":
                return "SnowflakeEtl";
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
            case "SnowflakeEtl":
                return "Snowflake";
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
            case "Snowflake":
                return "Snowflake";
            case "Olap":
                return "OLAP";
            case "ElasticSearch":
                return "Elasticsearch";
            case "Kafka":
                return "Kafka";
            case "RabbitMQ":
                return "RabbitMQ";
            case "AzureQueueStorage":
                return "Azure Queue Storage";
            default:
                assertUnreachable(etlType);
        }
    }
}

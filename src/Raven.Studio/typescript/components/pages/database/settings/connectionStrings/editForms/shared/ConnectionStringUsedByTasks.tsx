import React from "react";
import { ConnectionStringUsage, StudioConnectionType } from "../../connectionStringsTypes";
import { Icon } from "components/common/Icon";
import { CounterBadge } from "components/common/CounterBadge";
import { FormLabel } from "components/common/Form";
import { useAppUrls } from "components/hooks/useAppUrls";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";

interface ConnectionStringUsedByTasksProps {
    tasks: ConnectionStringUsage[];
    connectionType: StudioConnectionType;
}

export default function ConnectionStringUsedByTasks({ tasks, connectionType }: ConnectionStringUsedByTasksProps) {
    const { appUrl } = useAppUrls();
    const activeDatabaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    if (!tasks || tasks.length === 0) {
        return null;
    }

    const getQueueEtlUrl = (databaseName: string, id: number): string | null => {
        switch (connectionType) {
            case "Kafka":
                return appUrl.forEditKafkaEtl(databaseName, id);
            case "RabbitMQ":
                return appUrl.forEditRabbitMqEtl(databaseName, id);
            case "AzureQueueStorage":
                return appUrl.forEditAzureQueueStorageEtl(databaseName, id);
            case "AmazonSqs":
                return appUrl.forEditAmazonSqsEtl(databaseName, id);
            default:
                return null;
        }
    };

    const getQueueSinkUrl = (databaseName: string, id: number): string | null => {
        switch (connectionType) {
            case "Kafka":
                return appUrl.forEditKafkaSink(databaseName, id);
            case "RabbitMQ":
                return appUrl.forEditRabbitMqSink(databaseName, id);
            default:
                // Azure Queue Storage / Amazon SQS do not have a dedicated sink edit view
                return null;
        }
    };

    const getUrl = (usage: ConnectionStringUsage): string | null => {
        // For server-wide connection strings the usage carries its own database; otherwise it is the active one.
        const databaseName = usage.databaseName ?? activeDatabaseName;
        const id = usage.id;

        switch (usage.kind) {
            case "RavenEtl":
                return id != null ? appUrl.forEditRavenEtl(databaseName, id) : null;
            case "SqlEtl":
                return id != null ? appUrl.forEditSqlEtl(databaseName, id) : null;
            case "OlapEtl":
                return id != null ? appUrl.forEditOlapEtl(databaseName, id) : null;
            case "ElasticSearchEtl":
                return id != null ? appUrl.forEditElasticSearchEtl(databaseName, id) : null;
            case "SnowflakeEtl":
                return id != null ? appUrl.forEditSnowflakeEtl(databaseName, id) : null;
            case "ExternalReplication":
                return id != null ? appUrl.forEditExternalReplication(databaseName, id) : null;
            case "PullReplicationAsSink":
                return id != null ? appUrl.forEditReplicationSink(databaseName, id) : null;
            case "EmbeddingsGeneration":
                return id != null ? appUrl.forEditEmbeddingsGeneration(databaseName, id) : null;
            case "GenAi":
                return id != null ? appUrl.forEditGenAi(databaseName, id) : null;
            case "AiAgent":
                return usage.identifier ? appUrl.forEditAiAgent(databaseName, usage.identifier) : null;
            case "QueueEtl":
                return id != null ? getQueueEtlUrl(databaseName, id) : null;
            case "QueueSink":
                return id != null ? getQueueSinkUrl(databaseName, id) : null;
            default:
                return null;
        }
    };

    return (
        <div className="mt-2">
            <FormLabel className="d-flex align-items-center gap-1">
                Used in tasks <CounterBadge count={tasks.length} />
            </FormLabel>
            <div className="d-flex flex-wrap gap-2">
                {tasks.map((task, index) => {
                    const url = getUrl(task);
                    const key = task.identifier ?? `${task.databaseName ?? ""}-${task.kind}-${task.id ?? index}`;
                    // server-wide usages span databases, so disambiguate by appending the database name
                    const label = task.databaseName ? `${task.name} (${task.databaseName})` : task.name;

                    if (url) {
                        return (
                            <a key={key} href={url} className="btn btn-primary rounded-pill" title={label}>
                                <Icon icon="ongoing-tasks" />
                                {label}
                            </a>
                        );
                    }

                    return (
                        <span key={key} className="btn btn-primary rounded-pill disabled" title={label}>
                            <Icon icon="ongoing-tasks" />
                            {label}
                        </span>
                    );
                })}
            </div>
        </div>
    );
}

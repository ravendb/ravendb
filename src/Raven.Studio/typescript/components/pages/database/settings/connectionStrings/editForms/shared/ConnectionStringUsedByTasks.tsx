import React from "react";
import { ConnectionStringUsage, StudioConnectionType } from "../../connectionStringsTypes";
import { Icon } from "components/common/Icon";
import { CounterBadge } from "components/common/CounterBadge";
import { FormLabel } from "components/common/Form";
import { useAppUrls } from "components/hooks/useAppUrls";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import assertUnreachable from "components/utils/assertUnreachable";

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
            case "AzureServiceBus":
                return appUrl.forEditAzureServiceBusSink(databaseName, id);
            default:
                // Azure Queue Storage / Amazon SQS do not have a dedicated sink edit view
                return null;
        }
    };

    const getUrl = (usage: ConnectionStringUsage) => {
        // For server-wide connection strings the usage carries its own database; otherwise it is the active one.
        const databaseName = usage.databaseName ?? activeDatabaseName;
        const id = usage.id;

        switch (usage.kind) {
            case "RavenEtl":
                return appUrl.forEditRavenEtl(databaseName, id);
            case "SqlEtl":
                return appUrl.forEditSqlEtl(databaseName, id);
            case "OlapEtl":
                return appUrl.forEditOlapEtl(databaseName, id);
            case "ElasticSearchEtl":
                return appUrl.forEditElasticSearchEtl(databaseName, id);
            case "SnowflakeEtl":
                return appUrl.forEditSnowflakeEtl(databaseName, id);
            case "ExternalReplication":
                return appUrl.forEditExternalReplication(databaseName, id);
            case "PullReplicationAsSink":
                return appUrl.forEditReplicationSink(databaseName, id);
            case "EmbeddingsGeneration":
                return appUrl.forEditEmbeddingsGeneration(databaseName, id);
            case "GenAi":
                return appUrl.forEditGenAi(databaseName, id);
            case "AiAgent":
                return appUrl.forEditAiAgent(databaseName, usage.identifier);
            case "QueueEtl":
                return getQueueEtlUrl(databaseName, id);
            case "QueueSink":
                return getQueueSinkUrl(databaseName, id);
            default:
                return assertUnreachable(usage.kind);
        }
    };

    return (
        <div className="mb-2">
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

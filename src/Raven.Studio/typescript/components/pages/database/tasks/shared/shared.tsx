import {
    AnyEtlOngoingTaskInfo,
    OngoingEtlTaskNodeInfo,
    OngoingTaskInfo,
    OngoingTaskSharedInfo,
} from "components/models/tasks";
import useBoolean from "hooks/useBoolean";
import React, { ReactNode, useCallback, useReducer, useState } from "react";
import router from "plugins/router";
import { RichPanelDetailItem, RichPanelName } from "components/common/RichPanel";
import Spinner from "react-bootstrap/Spinner";
import ButtonGroup from "react-bootstrap/ButtonGroup";
import { Icon } from "components/common/Icon";
import { OngoingTaskOperationConfirmType } from "./OngoingTaskOperationConfirm";
import assertUnreachable from "components/utils/assertUnreachable";
import messagePublisher from "common/messagePublisher";
import { useServices } from "components/hooks/useServices";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppSelector } from "components/store";
import Button from "react-bootstrap/Button";
import Dropdown from "react-bootstrap/Dropdown";

import {
    ongoingTasksReducer,
    ongoingTasksReducerInitializer,
} from "components/pages/database/tasks/ongoingTasks/partials/OngoingTasksReducer";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { getLicenseLimitReachStatus } from "components/utils/licenseLimitsUtils";
import { useAppUrls } from "hooks/useAppUrls";
import appUrl from "common/appUrl";
import { CounterBadge } from "components/common/CounterBadge";
import {
    TaskCardCategory,
    TaskCardDisabledCondition,
    TaskCardInfo,
} from "components/pages/database/tasks/shared/AddTaskCardList";
import { useTaskCardFilters } from "components/pages/database/tasks/shared/useTaskCardFilters";
import { ongoingTaskCapabilities, OngoingTaskTarget } from "./ongoingTaskCapabilities";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { getAccessRequiredMessage } from "components/utils/accessUtils";
import { StudioConnectionType } from "components/pages/database/settings/connectionStrings/connectionStringsTypes";
import {
    getServerWideShortName,
    serverWideConnectionStringPrefix,
} from "components/pages/database/settings/connectionStrings/connectionStringsUtils";
import ModifyOngoingTaskResult = Raven.Client.Documents.Operations.OngoingTasks.ModifyOngoingTaskResult;

export interface BaseOngoingTaskPanelProps<T extends OngoingTaskInfo> {
    data: T;
    isSelected: (id: number) => boolean;
    toggleSelection: (checked: boolean, taskName: OngoingTaskSharedInfo) => void;
    onToggleDetails?: (newState: boolean) => void;
    onTaskOperation: (type: OngoingTaskOperationConfirmType, taskSharedInfos: OngoingTaskSharedInfo[]) => void;
    isDeleting: (id: number) => boolean;
    isTogglingState: (id: number) => boolean;
}

export function useTasksOperations(editUrl: string, props: BaseOngoingTaskPanelProps<OngoingTaskInfo>) {
    const { onToggleDetails } = props;
    const { value: detailsVisible, toggle: toggleDetailsVisible } = useBoolean(false);

    const onEdit = useCallback(() => {
        router.navigate(editUrl);
    }, [editUrl]);

    const toggleDetails = useCallback(() => {
        toggleDetailsVisible();
        onToggleDetails?.(!detailsVisible);
    }, [onToggleDetails, toggleDetailsVisible, detailsVisible]);

    return {
        detailsVisible,
        toggleDetails,
        onEdit,
    };
}

export function OngoingTaskResponsibleNode(props: { task: OngoingTaskInfo }) {
    const { task } = props;
    const preferredMentor = task.shared.mentorNodeTag;
    const currentNode = task.shared.responsibleNodeTag;

    const db = useAppSelector(databaseSelectors.activeDatabase);

    if (db?.isSharded) {
        // for sharded databases there are multiple responsible nodes, so user
        // can see it inside details only
        return null;
    }

    const usingNotPreferredNode = preferredMentor && currentNode ? preferredMentor !== currentNode : false;

    if (currentNode) {
        return (
            <div className="node">
                {usingNotPreferredNode ? (
                    <>
                        <span className="text-danger pulse" title="User preferred node for this task">
                            <Icon icon="cluster-node" />
                            {preferredMentor}
                        </span>

                        <span className="text-success" title="Cluster node that is temporary responsible for this task">
                            <Icon icon="arrow-right" color="danger" className="pulse" />
                            {currentNode}
                        </span>
                    </>
                ) : (
                    <span
                        title={
                            task.shared.taskType === "PullReplicationAsHub"
                                ? "Hub node that is serving this Sink task"
                                : "Cluster node that is responsible for this task"
                        }
                    >
                        <Icon icon="cluster-node" />
                        {currentNode}
                    </span>
                )}
            </div>
        );
    }

    return (
        <div title="No node is currently handling this task">
            <Icon icon="cluster-node" /> N/A
        </div>
    );
}

export function OngoingTaskName(props: { task: OngoingTaskInfo; canEdit: boolean; editUrl: string }) {
    const { task, editUrl, canEdit } = props;
    return (
        <RichPanelName>
            {canEdit ? (
                <a href={editUrl} title={"Task name: " + task.shared.taskName}>
                    {task.shared.taskName}
                </a>
            ) : (
                <span className="text-muted">{task.shared.taskName}</span>
            )}
        </RichPanelName>
    );
}

interface OngoingTaskStatusProps {
    task: OngoingTaskInfo;
    canEdit: boolean;
    onTaskOperation: (type: OngoingTaskOperationConfirmType, taskSharedInfos: OngoingTaskSharedInfo[]) => void;
    isTogglingState: boolean;
    id?: string;
}

export function OngoingTaskStatus(props: OngoingTaskStatusProps) {
    const { task, canEdit, onTaskOperation, isTogglingState, id } = props;
    return (
        <Dropdown id={id}>
            <Dropdown.Toggle
                disabled={!canEdit || isTogglingState}
                variant={task.shared.taskState === "Disabled" ? "warning" : "secondary"}
            >
                {isTogglingState && <Spinner size="sm" />} {task.shared.taskState}
            </Dropdown.Toggle>
            <Dropdown.Menu>
                <Dropdown.Item onClick={() => onTaskOperation("enable", [task.shared])}>
                    <Icon icon="play" color="success" /> Enable
                </Dropdown.Item>
                <Dropdown.Item onClick={() => onTaskOperation("disable", [task.shared])}>
                    <Icon icon="stop" color="danger" />
                    Disable
                </Dropdown.Item>
            </Dropdown.Menu>
        </Dropdown>
    );
}

interface OngoingTaskActionsProps {
    canEdit: boolean;
    task: OngoingTaskInfo;
    toggleDetails?: () => void;
    onEdit: () => void;
    onTaskOperation: (type: OngoingTaskOperationConfirmType, taskSharedInfos: OngoingTaskSharedInfo[]) => void;
    isDeleting: boolean;
    isDetailsOpen?: boolean;
    isEtl?: boolean;
}

export function OngoingTaskActions(props: OngoingTaskActionsProps) {
    const { canEdit, task, onEdit, toggleDetails, onTaskOperation, isDeleting, isDetailsOpen, isEtl } = props;

    return (
        <div className="actions">
            <ButtonGroup>
                {!isEtl && (
                    <Button variant="secondary" onClick={toggleDetails} title="Click for details">
                        <Icon icon={isDetailsOpen ? "fold" : "unfold"} margin="m-0" />
                    </Button>
                )}
                {!task.shared.serverWide && (
                    <Button variant="secondary" onClick={onEdit} title="Edit task">
                        <Icon icon="edit" margin="m-0" />
                    </Button>
                )}
                {!task.shared.serverWide && (
                    <ButtonWithSpinner
                        variant="danger"
                        disabled={!canEdit}
                        isSpinning={isDeleting}
                        onClick={() => onTaskOperation("delete", [task.shared])}
                        title="Delete task"
                        spinnerMargin="m-0"
                        icon={{
                            icon: "trash",
                            margin: "m-0",
                        }}
                    ></ButtonWithSpinner>
                )}
            </ButtonGroup>
        </div>
    );
}

export function ConnectionStringItem(props: {
    canEdit: boolean;
    connectionStringName: string;
    connectionStringType: StudioConnectionType;
    databaseName: string;
    connectionStringDefined: boolean;
}) {
    const { canEdit, connectionStringDefined, connectionStringName, connectionStringType, databaseName } = props;

    const isServerWide = connectionStringName?.startsWith(serverWideConnectionStringPrefix);

    const connectionStringsUrl = isServerWide
        ? appUrl.forServerWideConnectionStrings(connectionStringType, getServerWideShortName(connectionStringName))
        : appUrl.forConnectionStrings(databaseName, connectionStringType, connectionStringName);

    if (connectionStringDefined) {
        return (
            <RichPanelDetailItem label="Connection String">
                {canEdit ? (
                    <a title="Connection string name" target="_blank" href={connectionStringsUrl}>
                        {connectionStringName}
                    </a>
                ) : (
                    <div>{connectionStringName}</div>
                )}
            </RichPanelDetailItem>
        );
    }

    return (
        <RichPanelDetailItem label="Connection String">
            <Icon icon="danger" color="danger" />
            <span className="text-danger">This connection string is not defined.</span>
        </RichPanelDetailItem>
    );
}

export function DestinationUrlItem({
    destinationUrl,
    label = "Destination URL",
}: {
    destinationUrl: string;
    label?: string;
}) {
    if (!destinationUrl) {
        return null;
    }

    return (
        <RichPanelDetailItem label={label}>
            <a href={destinationUrl} target="_blank">
                {destinationUrl}
            </a>
        </RichPanelDetailItem>
    );
}

// Mode is a [Flags] enum, so the bidirectional case arrives as a combined string (e.g. "HubToSink, SinkToHub").
// Use includes() to stay robust to the flag formatting and render a readable label.
export function formatReplicationMode(mode: Raven.Client.Documents.Operations.Replication.PullReplicationMode): string {
    const hubToSink = mode?.includes("HubToSink");
    const sinkToHub = mode?.includes("SinkToHub");

    if (hubToSink && sinkToHub) {
        return "Hub to Sink & Sink to Hub";
    }
    if (hubToSink) {
        return "Hub to Sink";
    }
    if (sinkToHub) {
        return "Sink to Hub";
    }
    return null;
}

export function EmptyScriptsWarning(props: { task: AnyEtlOngoingTaskInfo }) {
    const emptyScripts = findScriptsWithOutMatchingDocuments(props.task);

    if (!emptyScripts.length) {
        return null;
    }

    return (
        <RichPanelDetailItem className="text-warning">
            <small>
                <Icon icon="warning" />
                Following scripts don&apos;t match any documents: {emptyScripts.join(", ")}
            </small>
        </RichPanelDetailItem>
    );
}

function findScriptsWithOutMatchingDocuments(
    data: OngoingTaskInfo<OngoingTaskSharedInfo, OngoingEtlTaskNodeInfo>
): string[] {
    const perScriptCounts = new Map<string, number>();
    data.nodesInfo.forEach((node) => {
        if (node.etlProgress) {
            node.etlProgress.forEach((progress) => {
                const transformationName = progress.transformationName;
                perScriptCounts.set(
                    transformationName,
                    (perScriptCounts.get(transformationName) ?? 0) + progress.global.total
                );
            });
        }
    });

    return Array.from(perScriptCounts.entries())
        .filter((x) => x[1] === 0)
        .map((x) => x[0]);
}

export function taskKey(task: OngoingTaskSharedInfo) {
    // we don't want to use taskId here - as it changes after edit
    return task.taskType + "-" + task.taskName;
}

interface OperationConfirm {
    type: OngoingTaskOperationConfirmType;
    onConfirm: () => void;
    taskSharedInfos: OngoingTaskSharedInfo[];
}

export function useOngoingTasksOperations(reload: () => void) {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { tasksService } = useServices();

    const [togglingTaskIds, setTogglingTaskIds] = useState<number[]>([]);
    const [deletingTaskIds, setDeletingTaskIds] = useState<number[]>([]);

    const [operationConfirm, setOperationConfirm] = useState<OperationConfirm>(null);

    const toggleOngoingTasks = async (enable: boolean, taskSharedInfos: OngoingTaskSharedInfo[]) => {
        try {
            setTogglingTaskIds((ids) => [...ids, ...taskSharedInfos.map((x) => x.taskId)]);
            const toggleRequests: Promise<ModifyOngoingTaskResult>[] = [];

            for (const task of taskSharedInfos) {
                if ((task.taskState === "Enabled" || task.taskState === "PartiallyEnabled") && enable) {
                    continue;
                }
                if (task.taskState === "Disabled" && !enable) {
                    continue;
                }

                toggleRequests.push(tasksService.toggleOngoingTask(databaseName, task, enable));
            }

            if (toggleRequests.length === 0) {
                return;
            }

            await Promise.all(toggleRequests);
            messagePublisher.reportSuccess(
                `${toggleRequests.length === 1 ? "Task" : "Tasks"} ${enable ? "enabled" : "disabled"} successfully.`
            );
            reload();
        } finally {
            setTogglingTaskIds((ids) => ids.filter((x) => !taskSharedInfos.map((x) => x.taskId).includes(x)));
        }
    };

    const deleteOngoingTasks = async (taskSharedInfos: OngoingTaskSharedInfo[]) => {
        try {
            setDeletingTaskIds((ids) => [...ids, ...taskSharedInfos.map((x) => x.taskId)]);

            const deleteRequests: Promise<ModifyOngoingTaskResult>[] = taskSharedInfos.map((task) =>
                tasksService.deleteOngoingTask(databaseName, task)
            );

            await Promise.all(deleteRequests);

            messagePublisher.reportSuccess(`${deleteRequests.length === 1 ? "Task" : "Tasks"} deleted successfully.`);
            reload();
        } finally {
            setDeletingTaskIds((ids) => ids.filter((x) => !taskSharedInfos.map((x) => x.taskId).includes(x)));
        }
    };

    const onTaskOperation = (type: OngoingTaskOperationConfirmType, taskSharedInfos: OngoingTaskSharedInfo[]) => {
        switch (type) {
            case "enable": {
                setOperationConfirm({
                    type: "enable",
                    onConfirm: () => toggleOngoingTasks(true, taskSharedInfos),
                    taskSharedInfos,
                });
                break;
            }
            case "disable": {
                setOperationConfirm({
                    type: "disable",
                    onConfirm: () => toggleOngoingTasks(false, taskSharedInfos),
                    taskSharedInfos,
                });
                break;
            }
            case "delete": {
                setOperationConfirm({
                    type: "delete",
                    onConfirm: () => deleteOngoingTasks(taskSharedInfos),
                    taskSharedInfos,
                });
                break;
            }
            default:
                assertUnreachable(type);
        }
    };

    return {
        onTaskOperation,
        operationConfirm,
        cancelOperationConfirm: () => setOperationConfirm(null),
        isDeleting: (id: number) => deletingTaskIds.includes(id),
        isTogglingState: (id: number) => togglingTaskIds.includes(id),
        isDeletingAny: deletingTaskIds.length > 0,
        isTogglingStateAny: togglingTaskIds.length > 0,
    };
}

export function useNewOngoingTasks({ isAiOnly = false }: { isAiOnly?: boolean }) {
    const db = useAppSelector(databaseSelectors.activeDatabase);
    const [tasks] = useReducer(ongoingTasksReducer, db, ongoingTasksReducerInitializer);

    const subscriptionsServerCount = useAppSelector(licenseSelectors.limitsUsage).NumberOfSubscriptionsInCluster;

    const license = useAppSelector(licenseSelectors.licenseInfo);
    const isProfessionalOrAbove = license.isAtLeast("Professional");

    const licenseStatus = useAppSelector(licenseSelectors.status);

    const subscriptionsServerLimit = useAppSelector(licenseSelectors.statusValue("MaxNumberOfSubscriptionsPerCluster"));
    const subscriptionsDatabaseLimit = useAppSelector(
        licenseSelectors.statusValue("MaxNumberOfSubscriptionsPerDatabase")
    );

    const subscriptionsServerLimitStatus = getLicenseLimitReachStatus(
        subscriptionsServerCount,
        subscriptionsServerLimit
    );

    const subscriptionsDatabaseLimitStatus = getLicenseLimitReachStatus(
        tasks.subscriptions.length,
        subscriptionsDatabaseLimit
    );

    const isSubscriptionDisabled =
        !isProfessionalOrAbove &&
        (subscriptionsServerLimitStatus === "limitReached" || subscriptionsDatabaseLimitStatus === "limitReached");

    const { forCurrentDatabase } = useAppUrls();

    const getSubscriptionLimitReason = () => {
        if (!isSubscriptionDisabled) {
            return null;
        }

        const limitReachedReason = subscriptionsServerLimitStatus === "limitReached" ? "Cluster" : "Database";

        return `${limitReachedReason} has reached the maximum number of subscriptions allowed per ${limitReachedReason.toLowerCase()}.`;
    };

    const isSharded = db?.isSharded;
    const getCanHandleOperation = useAppSelector(accessManagerSelectors.getCanHandleOperation);

    const getDisabledConditions = (opts: {
        accessRequired: databaseAccessLevel;
        isShardingSupported?: boolean;
        customDisabledReason?: ReactNode;
    }): TaskCardDisabledCondition[] => [
        {
            isActive: !getCanHandleOperation(opts.accessRequired),
            message: getAccessRequiredMessage(opts.accessRequired),
        },
        {
            isActive: !opts.isShardingSupported && isSharded,
            message: "Sharding is not supported for this task",
        },
        {
            isActive: !!opts.customDisabledReason,
            message: opts.customDisabledReason,
        },
    ];

    // Licence badge, sharding support and required access all come from ongoingTaskCapabilities,
    // shared with the import-from-file restrictions.
    const capabilitiesOf = (
        target: OngoingTaskTarget,
        customDisabledReason?: ReactNode
    ): Pick<TaskCardInfo, "licenseBadge" | "showLicenseBadge" | "disabledConditions"> => {
        const { licenseFlags, licenseBadge, isShardingSupported, accessRequired } = ongoingTaskCapabilities[target];

        return {
            licenseBadge,
            showLicenseBadge: licenseFlags.length > 0 && licenseFlags.every((flag) => !licenseStatus?.[flag]),
            disabledConditions: getDisabledConditions({ accessRequired, isShardingSupported, customDisabledReason }),
        };
    };

    let ongoingTasks: TaskCardCategory[] = [
        {
            categoryName: "AI",
            categoryIcon: "ai",
            tasks: [
                {
                    title: "GenAI",
                    description: "Analyze and enrich your documents using an LLM.",
                    iconName: "genai",
                    variant: "AI",
                    target: "GenAi",
                    link: forCurrentDatabase.editGenAiTaskUrl(),
                    ...capabilitiesOf("GenAi"),
                },
                {
                    title: "Embeddings Generation",
                    description: "Automatically generate embeddings from your document content.",
                    iconName: "ai-etl",
                    variant: "AI",
                    target: "EmbeddingsGeneration",
                    link: forCurrentDatabase.editEmbeddingsGenerationTaskUrl(),
                    ...capabilitiesOf("EmbeddingsGeneration"),
                },
            ],
        },
        {
            categoryName: "Replication",
            categoryIcon: "replication",
            tasks: [
                {
                    title: "External Replication",
                    description:
                        "Create a live replica of your database in another RavenDB database in another cluster.",
                    iconName: "external-replication",
                    variant: "Replication",
                    target: "ExternalReplication",
                    link: forCurrentDatabase.editExternalReplicationTaskUrl(),
                    ...capabilitiesOf("ExternalReplication"),
                },
                {
                    title: "Replication Hub",
                    description:
                        "Replicate documents to and/or from multiple Replication Sink tasks in other RavenDB databases across different clusters.",
                    iconName: "pull-replication-hub",
                    variant: "Replication",
                    target: "ReplicationHub",
                    link: forCurrentDatabase.editReplicationHubTaskUrl(),
                    ...capabilitiesOf("ReplicationHub"),
                },
                {
                    title: "Replication Sink",
                    description:
                        "Connect to a central Replication Hub in another RavenDB cluster to receive documents, and optionally replicate back.",
                    iconName: "pull-replication-agent",
                    variant: "Replication",
                    target: "ReplicationSink",
                    link: forCurrentDatabase.editReplicationSinkTaskUrl(),
                    ...capabilitiesOf("ReplicationSink"),
                },
            ],
        },
        {
            categoryName: "Backups",
            categoryIcon: "backup",
            tasks: [
                {
                    title: "Periodic Backup",
                    description: "Create periodic backups or snapshots of the database on a defined schedule.",
                    iconName: "periodic-backup",
                    variant: "Backup",
                    target: "PeriodicBackup",
                    link: forCurrentDatabase.editPeriodicBackupTask("OngoingTasks", false)(),
                    ...capabilitiesOf("PeriodicBackup"),
                },
            ],
        },
        {
            categoryName: "Subscriptions",
            categoryIcon: "subscriptions",
            tasks: [
                {
                    title: "Subscription",
                    description: "Send batches of documents that match a pre-defined query to a client for processing.",
                    iconName: "subscriptions",
                    variant: "Subscription",
                    target: "Subscription",
                    link: forCurrentDatabase.editSubscriptionTaskUrl(),
                    ...capabilitiesOf("Subscription", getSubscriptionLimitReason()),
                    counterBadge: isProfessionalOrAbove ? null : (
                        <CounterBadge
                            count={tasks.subscriptions.length}
                            limit={subscriptionsDatabaseLimit}
                            hideNotReached
                        />
                    ),
                },
            ],
        },
        {
            categoryName: "ETL",
            categoryHeaderName: "ETL (RavenDB ⇛ TARGET)",
            categoryIcon: "etl",
            tasks: [
                {
                    title: "RavenDB ETL",
                    description:
                        "Extract and transform selected database documents and write them to another RavenDB database.",
                    iconName: "ravendb-etl",
                    variant: "ETL",
                    target: "RavenETL",
                    link: forCurrentDatabase.editRavenEtlTaskUrl(),
                    ...capabilitiesOf("RavenETL"),
                },
                {
                    title: "Elasticsearch ETL",
                    description:
                        "Extract and transform data from selected documents and transfer it to an Elasticsearch destination.",
                    iconName: "elastic-search-etl",
                    variant: "ETL",
                    target: "ElasticSearchETL",
                    link: forCurrentDatabase.editElasticSearchEtlTaskUrl(),
                    ...capabilitiesOf("ElasticSearchETL"),
                },
                {
                    title: "Kafka ETL",
                    description: "Extract and transform data from selected documents and send it to Kafka topics.",
                    iconName: "kafka-etl",
                    variant: "ETL",
                    target: "KafkaETL",
                    link: forCurrentDatabase.editKafkaEtlTaskUrl(),
                    ...capabilitiesOf("KafkaETL"),
                },
                {
                    title: "SQL ETL",
                    description:
                        "Extract and transform data from selected documents and write it to a relational database.",
                    iconName: "sql-etl",
                    variant: "ETL",
                    target: "SqlETL",
                    link: forCurrentDatabase.editSqlEtlTaskUrl(),
                    ...capabilitiesOf("SqlETL"),
                },
                {
                    title: "Snowflake ETL",
                    description:
                        "Extract and transform data from selected documents and write it to a Snowflake database.",
                    iconName: "snowflake-etl",
                    variant: "ETL",
                    target: "SnowflakeETL",
                    link: forCurrentDatabase.editSnowflakeEtlTaskUrl(),
                    ...capabilitiesOf("SnowflakeETL"),
                },
                {
                    title: "OLAP ETL",
                    description:
                        "Extract and transform data from selected documents and export it as Parquet files to the specified destination.",
                    iconName: "olap-etl",
                    variant: "ETL",
                    target: "OlapETL",
                    link: forCurrentDatabase.editOlapEtlTaskUrl(),
                    ...capabilitiesOf("OlapETL"),
                },
                {
                    title: "RabbitMQ ETL",
                    description:
                        "Extract and transform data from selected documents and send it to a RabbitMQ exchange.",
                    iconName: "rabbitmq-etl",
                    variant: "ETL",
                    target: "RabbitMqETL",
                    link: forCurrentDatabase.editRabbitMqEtlTaskUrl(),
                    ...capabilitiesOf("RabbitMqETL"),
                },
                {
                    title: "Azure Queue Storage ETL",
                    description:
                        "Extract and transform data from selected documents and send it to Azure Queue Storage.",
                    iconName: "azure-queue-storage-etl",
                    variant: "ETL",
                    target: "AzureQueueStorageETL",
                    link: forCurrentDatabase.editAzureQueueStorageEtlTaskUrl(),
                    ...capabilitiesOf("AzureQueueStorageETL"),
                },
                {
                    title: "Amazon SQS ETL",
                    description: "Extract and transform data from selected documents and send it to Amazon SQS queues.",
                    iconName: "amazon-sqs-etl",
                    variant: "ETL",
                    target: "AmazonSqsETL",
                    link: forCurrentDatabase.editAmazonSqsEtlTaskUrl(),
                    ...capabilitiesOf("AmazonSqsETL"),
                },
            ],
        },
        {
            categoryName: "Sink",
            categoryHeaderName: "Sink (SOURCE ⇛ RavenDB)",
            categoryIcon: "hub-sink-replication",
            tasks: [
                {
                    title: "Kafka Sink",
                    description:
                        "Consume and process incoming JSON messages from Kafka topics to create or delete documents.",
                    iconName: "kafka-sink",
                    variant: "Sink",
                    target: "KafkaSink",
                    link: forCurrentDatabase.editKafkaSinkTaskUrl(),
                    ...capabilitiesOf("KafkaSink"),
                },
                {
                    title: "RabbitMQ Sink",
                    description:
                        "Consume and process incoming JSON messages from RabbitMQ queues to create or delete documents.",
                    iconName: "rabbitmq-sink",
                    target: "RabbitMqSink",
                    variant: "Sink",
                    link: forCurrentDatabase.editRabbitMqSinkTaskUrl(),
                    ...capabilitiesOf("RabbitMqSink"),
                },
                {
                    title: "Azure Service Bus Sink",
                    description:
                        "Consume and process incoming JSON messages from Azure Service Bus queues to create or delete documents.",
                    iconName: "azure",
                    target: "AzureServiceBusSink",
                    variant: "Sink",
                    link: forCurrentDatabase.editAzureServiceBusSinkTaskUrl(),
                    ...capabilitiesOf("AzureServiceBusSink"),
                },
                {
                    title: "CDC Sink",
                    description:
                        "Consume Change Data Capture streams from relational databases and apply inserts, updates, and deletes to documents in RavenDB.",
                    iconName: "sql-etl",
                    target: "CdcSink",
                    variant: "Sink",
                    link: forCurrentDatabase.editCdcSinkTaskUrl(),
                    ...capabilitiesOf("CdcSink"),
                },
            ],
        },
    ];

    if (isAiOnly) {
        ongoingTasks = ongoingTasks.filter((x) => x.categoryName === "AI");
    }

    return useTaskCardFilters(ongoingTasks);
}

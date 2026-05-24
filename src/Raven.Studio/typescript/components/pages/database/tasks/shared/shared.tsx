import {
    AnyEtlOngoingTaskInfo,
    OngoingEtlTaskNodeInfo,
    OngoingTaskInfo,
    OngoingTaskSharedInfo,
} from "components/models/tasks";
import useBoolean from "hooks/useBoolean";
import React, { useCallback, useReducer, useState } from "react";
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
import { InputItem } from "components/models/common";
import {
    ongoingTasksReducer,
    ongoingTasksReducerInitializer,
} from "components/pages/database/tasks/ongoingTasks/partials/OngoingTasksReducer";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { getLicenseLimitReachStatus } from "components/utils/licenseLimitsUtils";
import { useAppUrls } from "hooks/useAppUrls";
import { CounterBadge } from "components/common/CounterBadge";
import IconName from "../../../../../../typings/server/icons";
import { TaskItemProps } from "components/pages/database/tasks/ongoingTasks/AddNewOngoingTask";
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

export interface ICanShowTransformationScriptPreview {
    showItemPreview: (task: OngoingTaskInfo, scriptName: string) => void;
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
    connectionStringsUrl: string;
    connectionStringDefined: boolean;
}) {
    const { canEdit, connectionStringDefined, connectionStringName, connectionStringsUrl } = props;

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

interface OngoingTasksCategory {
    categoryName: string;
    categoryIcon: IconName;
    tasks: TaskItemProps[];
}

export function useNewOngoingTasks({ isAiOnly = false }: { isAiOnly?: boolean }) {
    const db = useAppSelector(databaseSelectors.activeDatabase);
    const [tasks] = useReducer(ongoingTasksReducer, db, ongoingTasksReducerInitializer);

    const subscriptionsServerCount = useAppSelector(licenseSelectors.limitsUsage).NumberOfSubscriptionsInCluster;

    const license = useAppSelector(licenseSelectors.licenseInfo);
    const isProfessionalOrAbove = license.isAtLeast("Professional");

    const hasExternalReplication = useAppSelector(licenseSelectors.statusValue("HasExternalReplication"));
    const hasReplicationHub = useAppSelector(licenseSelectors.statusValue("HasPullReplicationAsHub"));
    const hasReplicationSink = useAppSelector(licenseSelectors.statusValue("HasPullReplicationAsSink"));
    const hasRavenDbEtl = useAppSelector(licenseSelectors.statusValue("HasRavenEtl"));
    const hasElasticSearchEtl = useAppSelector(licenseSelectors.statusValue("HasElasticSearchEtl"));
    const hasKafkaEtl = useAppSelector(licenseSelectors.statusValue("HasQueueEtl"));
    const hasSqlEtl = useAppSelector(licenseSelectors.statusValue("HasSqlEtl"));
    const hasSnowflakeEtl = useAppSelector(licenseSelectors.statusValue("HasSnowflakeEtl"));
    const hasOlapEtl = useAppSelector(licenseSelectors.statusValue("HasOlapEtl"));
    const hasRabbitMqEtl = useAppSelector(licenseSelectors.statusValue("HasQueueEtl"));
    const hasAzureQueueStorageEtl = useAppSelector(licenseSelectors.statusValue("HasQueueEtl"));
    const hasAmazonSqsEtl = useAppSelector(licenseSelectors.statusValue("HasQueueEtl"));
    const hasKafkaSink = useAppSelector(licenseSelectors.statusValue("HasQueueSink"));
    const hasRabbitMqSink = useAppSelector(licenseSelectors.statusValue("HasQueueSink"));
    const hasCdcSink = useAppSelector(licenseSelectors.statusValue("HasCdcSink"));
    const hasPeriodicBackups = useAppSelector(licenseSelectors.statusValue("HasPeriodicBackup"));
    const hasGenAi = useAppSelector(licenseSelectors.statusValue("HasGenAi"));
    const hasEmbeddingGeneration = useAppSelector(licenseSelectors.statusValue("HasEmbeddingsGeneration"));

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

    const [selectedCategories, setSelectedCategories] = useState<string[]>([]);
    const [searchText, setSearchText] = useState<string>("");
    const { forCurrentDatabase } = useAppUrls();

    const getSubscriptionLimitReason = () => {
        if (!isSubscriptionDisabled) {
            return null;
        }

        const limitReachedReason = subscriptionsServerLimitStatus === "limitReached" ? "Cluster" : "Database";

        return `${limitReachedReason} has reached the maximum number of subscriptions allowed per ${limitReachedReason.toLowerCase()}.`;
    };

    let ongoingTasks: OngoingTasksCategory[] = [
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
                    showLicenseBadge: !hasGenAi,
                    licenseBadge: "Enterprise AI",
                    link: forCurrentDatabase.editGenAiTaskUrl(),
                    accessRequired: "DatabaseAdmin",
                },
                {
                    title: "Embeddings Generation",
                    description: "Automatically generate embeddings from your document content.",
                    iconName: "ai-etl",
                    variant: "AI",
                    target: "EmbeddingsGeneration",
                    showLicenseBadge: !hasEmbeddingGeneration,
                    link: forCurrentDatabase.editEmbeddingsGenerationTaskUrl(),
                    isShardingSupported: true,
                    accessRequired: "DatabaseAdmin",
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
                    licenseBadge: "Professional +",
                    showLicenseBadge: !hasExternalReplication,
                    link: forCurrentDatabase.editExternalReplicationTaskUrl(),
                    isShardingSupported: true,
                    accessRequired: "DatabaseAdmin",
                },
                {
                    title: "Replication Hub",
                    description:
                        "Replicate documents to and/or from multiple Replication Sink tasks in other RavenDB databases across different clusters.",
                    iconName: "pull-replication-hub",
                    variant: "Replication",
                    licenseBadge: "Enterprise",
                    target: "ReplicationHub",
                    showLicenseBadge: !hasReplicationHub,
                    link: forCurrentDatabase.editReplicationHubTaskUrl(),
                    accessRequired: "DatabaseAdmin",
                },
                {
                    title: "Replication Sink",
                    description:
                        "Connect to a central Replication Hub in another RavenDB cluster to receive documents, and optionally replicate back.",
                    iconName: "pull-replication-agent",
                    variant: "Replication",
                    target: "ReplicationSink",
                    licenseBadge: "Professional +",
                    showLicenseBadge: !hasReplicationSink,
                    link: forCurrentDatabase.editReplicationSinkTaskUrl(),
                    accessRequired: "DatabaseAdmin",
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
                    variant: "Backups",
                    licenseBadge: "Professional +",
                    showLicenseBadge: !hasPeriodicBackups,
                    target: "PeriodicBackup",
                    link: forCurrentDatabase.editPeriodicBackupTask("OngoingTasks", false)(),
                    isShardingSupported: true,
                    accessRequired: "DatabaseAdmin",
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
                    variant: "Subscriptions",
                    target: "Subscription",
                    link: forCurrentDatabase.editSubscriptionTaskUrl(),
                    isShardingSupported: true,
                    accessRequired: "DatabaseReadWrite",
                    customDisabledReason: getSubscriptionLimitReason(),
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
            categoryName: "ETL (RavenDB ⇛ TARGET)",
            categoryIcon: "etl",
            tasks: [
                {
                    title: "RavenDB ETL",
                    description:
                        "Extract and transform selected database documents and write them to another RavenDB database.",
                    iconName: "ravendb-etl",
                    variant: "ETL",
                    target: "RavenETL",
                    licenseBadge: "Professional +",
                    showLicenseBadge: !hasRavenDbEtl,
                    link: forCurrentDatabase.editRavenEtlTaskUrl(),
                    isShardingSupported: true,
                    accessRequired: "DatabaseAdmin",
                },
                {
                    title: "Elasticsearch ETL",
                    description:
                        "Extract and transform data from selected documents and transfer it to an Elasticsearch destination.",
                    iconName: "elastic-search-etl",
                    variant: "ETL",
                    target: "ElasticSearchETL",
                    licenseBadge: "Enterprise",
                    showLicenseBadge: !hasElasticSearchEtl,
                    link: forCurrentDatabase.editElasticSearchEtlTaskUrl(),
                    isShardingSupported: true,
                    accessRequired: "DatabaseAdmin",
                },
                {
                    title: "Kafka ETL",
                    description: "Extract and transform data from selected documents and send it to Kafka topics.",
                    iconName: "kafka-etl",
                    variant: "ETL",
                    target: "KafkaETL",
                    licenseBadge: "Enterprise",
                    showLicenseBadge: !hasKafkaEtl,
                    link: forCurrentDatabase.editKafkaEtlTaskUrl(),
                    accessRequired: "DatabaseAdmin",
                },
                {
                    title: "SQL ETL",
                    description:
                        "Extract and transform data from selected documents and write it to a relational database.",
                    iconName: "sql-etl",
                    variant: "ETL",
                    target: "SqlETL",
                    licenseBadge: "Professional +",
                    showLicenseBadge: !hasSqlEtl,
                    link: forCurrentDatabase.editSqlEtlTaskUrl(),
                    isShardingSupported: true,
                    accessRequired: "DatabaseAdmin",
                },
                {
                    title: "Snowflake ETL",
                    description:
                        "Extract and transform data from selected documents and write it to a Snowflake database.",
                    iconName: "snowflake-etl",
                    variant: "ETL",
                    target: "SnowflakeETL",
                    licenseBadge: "Enterprise",
                    showLicenseBadge: !hasSnowflakeEtl,
                    link: forCurrentDatabase.editSnowflakeEtlTaskUrl(),
                    isShardingSupported: true,
                    accessRequired: "DatabaseAdmin",
                },
                {
                    title: "OLAP ETL",
                    description:
                        "Extract and transform data from selected documents and export it as Parquet files to the specified destination.",
                    iconName: "olap-etl",
                    variant: "ETL",
                    target: "OlapETL",
                    link: forCurrentDatabase.editOlapEtlTaskUrl(),
                    licenseBadge: "Enterprise",
                    showLicenseBadge: !hasOlapEtl,
                    isShardingSupported: true,
                    accessRequired: "DatabaseAdmin",
                },
                {
                    title: "RabbitMQ ETL",
                    description:
                        "Extract and transform data from selected documents and send it to a RabbitMQ exchange.",
                    iconName: "rabbitmq-etl",
                    variant: "ETL",
                    target: "RabbitMqETL",
                    licenseBadge: "Enterprise",
                    showLicenseBadge: !hasRabbitMqEtl,
                    link: forCurrentDatabase.editRabbitMqEtlTaskUrl(),
                    accessRequired: "DatabaseAdmin",
                },
                {
                    title: "Azure Queue Storage ETL",
                    description:
                        "Extract and transform data from selected documents and send it to Azure Queue Storage.",
                    iconName: "azure-queue-storage-etl",
                    variant: "ETL",
                    target: "AzureQueueStorageETL",
                    licenseBadge: "Enterprise",
                    showLicenseBadge: !hasAzureQueueStorageEtl,
                    link: forCurrentDatabase.editAzureQueueStorageEtlTaskUrl(),
                    accessRequired: "DatabaseAdmin",
                },
                {
                    title: "Amazon SQS ETL",
                    description: "Extract and transform data from selected documents and send it to Amazon SQS queues.",
                    iconName: "amazon-sqs-etl",
                    variant: "ETL",
                    target: "AmazonSqsETL",
                    licenseBadge: "Enterprise",
                    showLicenseBadge: !hasAmazonSqsEtl,
                    link: forCurrentDatabase.editAmazonSqsEtlTaskUrl(),
                    accessRequired: "DatabaseAdmin",
                },
            ],
        },
        {
            categoryName: "SINK (SOURCE ⇛ RavenDB)",
            categoryIcon: "hub-sink-replication",
            tasks: [
                {
                    title: "Kafka Sink",
                    description:
                        "Consume and process incoming JSON messages from Kafka topics to create or delete documents.",
                    iconName: "kafka-sink",
                    variant: "Sink",
                    target: "KafkaSink",
                    licenseBadge: "Enterprise",
                    showLicenseBadge: !hasKafkaSink,
                    link: forCurrentDatabase.editKafkaSinkTaskUrl(),
                    accessRequired: "DatabaseAdmin",
                },
                {
                    title: "RabbitMQ Sink",
                    description:
                        "Consume and process incoming JSON messages from RabbitMQ queues to create or delete documents.",
                    iconName: "rabbitmq-sink",
                    target: "RabbitMqSink",
                    variant: "Sink",
                    licenseBadge: "Enterprise",
                    showLicenseBadge: !hasRabbitMqSink,
                    link: forCurrentDatabase.editRabbitMqSinkTaskUrl(),
                    accessRequired: "DatabaseAdmin",
                },
                {
                    title: "CDC Sink",
                    description:
                        "Consume Change Data Capture streams from relational databases and apply inserts, updates, and deletes to documents in RavenDB.",
                    iconName: "sql-etl",
                    target: "CdcSink",
                    variant: "Sink",
                    licenseBadge: "Enterprise",
                    showLicenseBadge: !hasCdcSink,
                    link: forCurrentDatabase.editCdcSinkTaskUrl(),
                    accessRequired: "DatabaseAdmin",
                },
            ],
        },
    ];

    if (isAiOnly) {
        ongoingTasks = ongoingTasks.filter((x) => x.categoryName === "AI");
    }

    function getCategoryCount(category: OngoingTasksCategory["categoryName"]) {
        const categoryTasks = ongoingTasks.find((x) => x.categoryName === category)?.tasks ?? [];
        return categoryTasks.length;
    }

    const filteredTasks = ongoingTasks
        .map((category) => ({
            ...category,
            tasks: category.tasks.filter((task) => matchesSearchText(task, searchText)),
        }))
        .filter(
            (category) => isCategorySelected(category.categoryName, selectedCategories) && category.tasks.length > 0
        );

    const categoryList: InputItem[] = [
        { value: "AI", label: "AI", count: getCategoryCount("AI") },
        { value: "Replication", label: "Replication", count: getCategoryCount("Replication") },
        { value: "Backups", label: "Backups", count: getCategoryCount("Backups") },
        { value: "Subscriptions", label: "Subscriptions", count: getCategoryCount("Subscriptions") },
        { value: "ETL (RavenDB ⇛ TARGET)", label: "ETL", count: getCategoryCount("ETL (RavenDB ⇛ TARGET)") },
        { value: "SINK (SOURCE ⇛ RavenDB)", label: "Sink", count: getCategoryCount("SINK (SOURCE ⇛ RavenDB)") },
    ];

    return {
        filteredTasks,
        categoryList,
        searchText,
        selectedCategories,
        setSearchText,
        setSelectedCategories,
    };
}

const matchesSearchText = (task: TaskItemProps, searchText: string) => {
    if (!searchText) {
        return true;
    }

    const searchLower = searchText.trim().toLowerCase();
    return task.title.toLowerCase().includes(searchLower) || task.description.toLowerCase().includes(searchLower);
};

const isCategorySelected = (categoryName: string, selectedCategories: string[]) => {
    if (selectedCategories.length === 0) {
        return true;
    }

    const categoryLower = categoryName.toLowerCase();
    return selectedCategories.some((selected) => selected.toLowerCase() === categoryLower);
};

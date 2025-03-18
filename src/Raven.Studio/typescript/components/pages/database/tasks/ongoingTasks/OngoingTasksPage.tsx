import React, { useCallback, useEffect, useReducer, useState } from "react";
import { useServices } from "hooks/useServices";
import { OngoingTasksState, ongoingTasksReducer, ongoingTasksReducerInitializer } from "./partials/OngoingTasksReducer";
import { ExternalReplicationPanel } from "./panels/ExternalReplicationPanel";
import {
    OngoingTaskAmazonSqsEtlInfo,
    OngoingTaskAzureQueueStorageEtlInfo,
    OngoingTaskElasticSearchEtlInfo,
    OngoingTaskExternalReplicationInfo,
    OngoingTaskInfo,
    OngoingTaskKafkaEtlInfo,
    OngoingTaskKafkaSinkInfo,
    OngoingTaskOlapEtlInfo,
    OngoingTaskPeriodicBackupInfo,
    OngoingTaskRabbitMqEtlInfo,
    OngoingTaskRabbitMqSinkInfo,
    OngoingTaskRavenEtlInfo,
    OngoingTaskReplicationHubInfo,
    OngoingTaskReplicationSinkInfo,
    OngoingTaskSharedInfo,
    OngoingTaskSnowflakeEtlInfo,
    OngoingTaskSqlEtlInfo,
} from "components/models/tasks";
import { RavenEtlPanel } from "./panels/RavenEtlPanel";
import { SqlEtlPanel } from "./panels/SqlEtlPanel";
import { OlapEtlPanel } from "./panels/OlapEtlPanel";
import { ElasticSearchEtlPanel } from "./panels/ElasticSearchEtlPanel";
import { PeriodicBackupPanel } from "./panels/PeriodicBackupPanel";
import { SubscriptionPanel } from "./panels/SubscriptionPanel";
import { ReplicationSinkPanel } from "./panels/ReplicationSinkPanel";
import { ReplicationHubDefinitionPanel } from "./panels/ReplicationHubDefinitionPanel";
import useBoolean from "hooks/useBoolean";
import {
    EtlProgressProvider,
    InternalReplicationProgressProvider,
    ReplicationProgressProvider,
} from "./partials/OngoingTaskProgressProviders";
import { BaseOngoingTaskPanelProps, taskKey, useOngoingTasksOperations } from "../shared/shared";
import EtlTaskProgress = Raven.Server.Documents.ETL.Stats.EtlTaskProgress;
import "./OngoingTaskPage.scss";
import etlScriptDefinitionCache from "models/database/stats/etlScriptDefinitionCache";
import TaskUtils from "../../../../utils/TaskUtils";
import { KafkaEtlPanel } from "./panels/KafkaEtlPanel";
import { RabbitMqEtlPanel } from "./panels/RabbitMqEtlPanel";
import useInterval from "hooks/useInterval";
import Row from "react-bootstrap/Row";
import { HrHeader } from "components/common/HrHeader";
import { EmptySet } from "components/common/EmptySet";
import { Icon } from "components/common/Icon";
import { OngoingTasksFilterCriteria } from "./partials/OngoingTasksFilter";
import OngoingTaskOperationConfirm from "../shared/OngoingTaskOperationConfirm";
import { KafkaSinkPanel } from "components/pages/database/tasks/ongoingTasks/panels/KafkaSinkPanel";
import { RabbitMqSinkPanel } from "components/pages/database/tasks/ongoingTasks/panels/RabbitMqSinkPanel";
import { CounterBadge } from "components/common/CounterBadge";
import { getLicenseLimitReachStatus } from "components/utils/licenseLimitsUtils";
import { useAppSelector } from "components/store";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useRavenLink } from "components/hooks/useRavenLink";
import { throttledUpdateLicenseLimitsUsage } from "components/common/shell/setup";
import { AzureQueueStorageEtlPanel } from "components/pages/database/tasks/ongoingTasks/panels/AzureQueueStorageEtlPanel";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { compareSets } from "common/typeUtils";
import RichAlert from "components/common/RichAlert";
import ReplicationTaskProgress = Raven.Server.Documents.Replication.Stats.ReplicationTaskProgress;
import InternalReplicationTaskProgress = Raven.Server.Documents.Replication.Stats.InternalReplicationTaskProgress;
import { OngoingTasksHeader } from "components/pages/database/tasks/ongoingTasks/partials/OngoingTasksHeader";
import { InternalReplicationPanel } from "./panels/InternalReplicationPanel";
import DatabaseUtils from "components/utils/DatabaseUtils";
import recentError from "common/notifications/models/recentError";
import { SnowflakeEtlPanel } from "components/pages/database/tasks/ongoingTasks/panels/SnowflakeEtlPanel";
import { AmazonSqsEtlPanel } from "components/pages/database/tasks/ongoingTasks/panels/AmazonSqsEtlPanel";

export function OngoingTasksPage() {
    const db = useAppSelector(databaseSelectors.activeDatabase);

    const { tasksService } = useServices();
    const [tasks, dispatch] = useReducer(ongoingTasksReducer, db, ongoingTasksReducerInitializer);

    const { value: internalReplicationProgressEnabled, setTrue: startTrackingInternalReplicationProgress } =
        useBoolean(false);
    const { value: replicationProgressEnabled, setTrue: startTrackingReplicationProgress } = useBoolean(false);
    const { value: etlProgressEnabled, setTrue: startTrackingEtlProgress } = useBoolean(false);
    const [definitionCache] = useState(() => new etlScriptDefinitionCache(db.name));
    const [filter, setFilter] = useState<OngoingTasksFilterCriteria>({
        searchText: "",
        types: [],
    });

    const upgradeLicenseLink = useRavenLink({ hash: "FLDLO4", isDocs: false });

    const fetchTasks = useCallback(
        async (location: databaseLocationSpecifier) => {
            try {
                const tasks = await tasksService.getOngoingTasks(db.name, location);
                dispatch({
                    type: "TasksLoaded",
                    location,
                    tasks,
                });
            } catch (e) {
                const errorAndMessage = recentError.tryExtractMessageAndException(e.responseText);
                dispatch({
                    type: "TasksLoadError",
                    location,
                    error: errorAndMessage.message + (errorAndMessage.error ? ": " + errorAndMessage.error : ""),
                });
            }
        },
        [db, tasksService, dispatch]
    );

    const reload = useCallback(async () => {
        // if database is sharded we need to load from both orchestrator and target node point of view
        // in case of non-sharded - we have single level: node

        if (db.isSharded) {
            const orchestratorTasks = db.nodes.map((node) => fetchTasks({ nodeTag: node.tag }));
            await Promise.all(orchestratorTasks);
        }

        const loadTasks = tasks.locations.map(fetchTasks);
        await Promise.all(loadTasks);
    }, [tasks, fetchTasks, db]);

    useInterval(reload, 10_000);

    useEffect(() => {
        reload();
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [db]);

    const onEtlProgress = useCallback(
        (progress: EtlTaskProgress[], location: databaseLocationSpecifier) => {
            dispatch({
                type: "EtlProgressLoaded",
                progress,
                location,
            });
        },
        [dispatch]
    );

    const onReplicationProgress = useCallback(
        (progress: ReplicationTaskProgress[], location: databaseLocationSpecifier) => {
            dispatch({
                type: "ReplicationProgressLoaded",
                progress,
                location,
            });
        },
        [dispatch]
    );

    const onInternalReplicationProgress = useCallback(
        (progress: InternalReplicationTaskProgress[], location: databaseLocationSpecifier) => {
            dispatch({
                type: "InternalReplicationProgressLoaded",
                progress,
                location,
            });
        },
        [dispatch]
    );

    const onInternalReplicationError = useCallback(
        (error: string, location: databaseLocationSpecifier) => {
            dispatch({
                type: "InternalReplicationProgressError",
                error,
                location,
            });
        },
        [dispatch]
    );

    const showItemPreview = useCallback(
        (task: OngoingTaskInfo, scriptName: string) => {
            const taskType = TaskUtils.studioTaskTypeToTaskType(task.shared.taskType);
            const etlType = TaskUtils.taskTypeToEtlType(taskType);
            definitionCache.showDefinitionFor(etlType, task.shared.taskId, scriptName);
        },
        [definitionCache]
    );

    const filteredTasks = getFilteredTasks(tasks, filter);

    const {
        internalReplications,
        externalReplications,
        ravenEtls,
        sqlEtls,
        snowflakeEtls,
        olapEtls,
        kafkaEtls,
        rabbitMqEtls,
        azureQueueStorageEtls,
        amazonSqsEtls,
        kafkaSinks,
        rabbitMqSinks,
        elasticSearchEtls,
        backups,
        replicationHubs,
        replicationSinks,
        subscriptions,
        hubDefinitions,
    } = filteredTasks;

    useEffect(() => {
        throttledUpdateLicenseLimitsUsage();
    }, [subscriptions.length]);

    const {
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        replicationHubs: ignored,
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        internalReplications: ignored2,
        ...filteredWithoutReplicationHubs
    } = filteredTasks;
    const filteredDatabaseTaskIds = Object.values(filteredWithoutReplicationHubs)
        .flat()
        .filter((x) => !x.shared.serverWide)
        .map((x) => x.shared.taskId);

    const [selectedTaskIds, setSelectedTaskIds] = useState<number[]>(filteredDatabaseTaskIds);

    useEffect(() => {
        const updatedSelectedTaskIds = selectedTaskIds.filter((id) => filteredDatabaseTaskIds.includes(id));

        if (!compareSets(updatedSelectedTaskIds, selectedTaskIds)) {
            setSelectedTaskIds(updatedSelectedTaskIds);
        }
    }, [filteredDatabaseTaskIds, selectedTaskIds]);

    const allTasksCount =
        tasks.tasks.filter((x) => x.shared.taskType !== "PullReplicationAsHub").length +
        tasks.replicationHubs.length +
        tasks.subscriptions.length +
        (DatabaseUtils.hasInternalReplication(db) ? 1 : 0);
    const refreshSubscriptionInfo = async (taskId: number, taskName: string) => {
        const loadTasks = db.nodes.map(async (nodeInfo) => {
            const nodeTag = nodeInfo.tag;
            const task = await tasksService.getSubscriptionTaskInfo(db.name, taskId, taskName, nodeTag);

            dispatch({
                type: "SubscriptionInfoLoaded",
                nodeTag,
                task,
            });

            return task;
        });

        const taskInfoSettledResult = await Promise.allSettled(loadTasks);

        if (!taskInfoSettledResult.some((x) => x.status === "fulfilled")) {
            dispatch({
                type: "SubscriptionConnectionDetailsLoaded",
                subscriptionId: taskId,
                loadError: "Failed to get client connection details",
            });

            return;
        }

        const targetNode = taskInfoSettledResult
            .filter((x) => x.status === "fulfilled")
            .map((x) => x.value)
            .find((x) => x.ResponsibleNode.NodeTag);

        try {
            // ask only responsible node for connection details
            // if case of sharded database it points to responsible orchestrator
            const details = await tasksService.getSubscriptionConnectionDetails(
                db.name,
                taskId,
                taskName,
                targetNode.ResponsibleNode.NodeTag
            );

            dispatch({
                type: "SubscriptionConnectionDetailsLoaded",
                subscriptionId: taskId,
                details,
            });
        } catch (e) {
            dispatch({
                type: "SubscriptionConnectionDetailsLoaded",
                subscriptionId: taskId,
                loadError: "Failed to get client connection details",
            });
        }
    };

    const dropSubscription = async (taskId: number, taskName: string, nodeTag: string, workerId: string) => {
        await tasksService.dropSubscription(db.name, taskId, taskName, nodeTag, workerId);
    };

    const { onTaskOperation, operationConfirm, cancelOperationConfirm, isTogglingState, isDeleting } =
        useOngoingTasksOperations(reload);

    const sharedPanelProps: Omit<BaseOngoingTaskPanelProps<OngoingTaskInfo>, "data"> = {
        onTaskOperation,
        isSelected: (id: number) => selectedTaskIds.includes(id),
        toggleSelection: (checked: boolean, taskShardedInfo: OngoingTaskSharedInfo) => {
            if (checked) {
                setSelectedTaskIds((selectedIds) => [...selectedIds, taskShardedInfo.taskId]);
            } else {
                setSelectedTaskIds((selectedIds) => selectedIds.filter((x) => x !== taskShardedInfo.taskId));
            }
        },
        isTogglingState,
        isDeleting,
    };

    const subscriptionsServerCount = useAppSelector(licenseSelectors.limitsUsage).NumberOfSubscriptionsInCluster;
    const subscriptionsDatabaseCount = subscriptions.length;

    const subscriptionsClusterLimit = useAppSelector(
        licenseSelectors.statusValue("MaxNumberOfSubscriptionsPerCluster")
    );
    const subscriptionsDatabaseLimit = useAppSelector(
        licenseSelectors.statusValue("MaxNumberOfSubscriptionsPerDatabase")
    );

    const subscriptionsClusterLimitStatus = getLicenseLimitReachStatus(
        subscriptionsServerCount,
        subscriptionsClusterLimit
    );

    const subscriptionsDatabaseLimitStatus = getLicenseLimitReachStatus(
        subscriptionsDatabaseCount,
        subscriptionsDatabaseLimit
    );

    const showInternalReplication = DatabaseUtils.hasInternalReplication(db);

    return (
        <div className="content-margin">
            {subscriptionsClusterLimitStatus !== "notReached" && (
                <RichAlert
                    variant={subscriptionsClusterLimitStatus === "limitReached" ? "danger" : "warning"}
                    icon="cluster"
                    iconAddon="warning"
                    className="mb-3"
                >
                    Cluster {subscriptionsClusterLimitStatus === "limitReached" ? "reached" : "is reaching"} the{" "}
                    <strong>maximum number of subscriptions</strong> allowed per cluster by your license{" "}
                    <strong>
                        ({subscriptionsServerCount}/{subscriptionsClusterLimit})
                    </strong>
                    <br />
                    <strong>
                        <a href={upgradeLicenseLink} target="_blank">
                            Upgrade your license
                        </a>{" "}
                    </strong>
                    to add more
                </RichAlert>
            )}

            {subscriptionsDatabaseLimitStatus !== "notReached" && (
                <RichAlert
                    variant={subscriptionsDatabaseLimitStatus === "limitReached" ? "danger" : "warning"}
                    icon="database"
                    iconAddon="warning"
                    className="mb-3"
                >
                    Database {subscriptionsDatabaseLimitStatus === "limitReached" ? "reached" : "is reaching"} the{" "}
                    <strong>maximum number of subscriptions</strong> allowed per database by your license{" "}
                    <strong>
                        ({subscriptionsDatabaseCount}/{subscriptionsDatabaseLimit})
                    </strong>
                    <br />
                    <strong>
                        <a href={upgradeLicenseLink} target="_blank">
                            Upgrade your license
                        </a>{" "}
                    </strong>
                    to add more
                </RichAlert>
            )}

            {internalReplicationProgressEnabled && (
                <InternalReplicationProgressProvider
                    key="internalReplicationProgress"
                    onProgress={onInternalReplicationProgress}
                    onError={onInternalReplicationError}
                />
            )}
            {replicationProgressEnabled && (
                <ReplicationProgressProvider key="replicationProgress" onProgress={onReplicationProgress} />
            )}
            {etlProgressEnabled && <EtlProgressProvider key="etlProgressEnabled" onProgress={onEtlProgress} />}
            {operationConfirm && <OngoingTaskOperationConfirm {...operationConfirm} toggle={cancelOperationConfirm} />}
            <OngoingTasksHeader
                reload={reload}
                allTasksCount={allTasksCount}
                tasks={tasks}
                hasInternalReplication={DatabaseUtils.hasInternalReplication(db)}
                selectedTaskIds={selectedTaskIds}
                subscriptionsDatabaseCount={subscriptionsDatabaseCount}
                filter={filter}
                setFilter={setFilter}
                setSelectedTaskIds={setSelectedTaskIds}
                filteredDatabaseTaskIds={filteredDatabaseTaskIds}
            />
            <Row className="gy-sm">
                <div className="flex-vertical">
                    <div className="scroll flex-grow">
                        {allTasksCount === 0 && !showInternalReplication && (
                            <EmptySet>No tasks have been created for this Database Group.</EmptySet>
                        )}
                        {showInternalReplication && internalReplications.length > 0 && (
                            <InternalReplicationPanel
                                onToggleDetails={startTrackingInternalReplicationProgress}
                                data={tasks.internalReplication}
                            />
                        )}

                        {externalReplications.length > 0 && (
                            <div key="external-replications" data-testid="external-replications">
                                <HrHeader className="external-replication" count={externalReplications.length}>
                                    <Icon icon="external-replication" /> External Replication
                                </HrHeader>

                                {externalReplications.map((x) => (
                                    <ExternalReplicationPanel
                                        {...sharedPanelProps}
                                        key={taskKey(x.shared)}
                                        data={x}
                                        onToggleDetails={startTrackingReplicationProgress}
                                    />
                                ))}
                            </div>
                        )}
                        {ravenEtls.length > 0 && (
                            <div key="raven-etls" data-testid="raven-etls">
                                <HrHeader className="ravendb-etl" count={ravenEtls.length}>
                                    <Icon icon="etl" />
                                    RavenDB ETL
                                </HrHeader>

                                {ravenEtls.map((x) => (
                                    <RavenEtlPanel
                                        {...sharedPanelProps}
                                        key={taskKey(x.shared)}
                                        data={x}
                                        onToggleDetails={startTrackingEtlProgress}
                                        showItemPreview={showItemPreview}
                                    />
                                ))}
                            </div>
                        )}
                        {sqlEtls.length > 0 && (
                            <div key="sql-etls" data-testid="sql-etls">
                                <HrHeader className="sql-etl" count={sqlEtls.length}>
                                    <Icon icon="sql-etl" />
                                    SQL ETL
                                </HrHeader>

                                {sqlEtls.map((x) => (
                                    <SqlEtlPanel
                                        {...sharedPanelProps}
                                        key={taskKey(x.shared)}
                                        data={x}
                                        onToggleDetails={startTrackingEtlProgress}
                                        showItemPreview={showItemPreview}
                                    />
                                ))}
                            </div>
                        )}
                        {snowflakeEtls.length > 0 && (
                            <div key="snowflake-etls">
                                <HrHeader className="snowflake-etl" count={snowflakeEtls.length}>
                                    <Icon icon="snowflake-etl" />
                                    Snowflake ETL
                                </HrHeader>

                                {snowflakeEtls.map((x) => (
                                    <SnowflakeEtlPanel
                                        {...sharedPanelProps}
                                        key={taskKey(x.shared)}
                                        data={x}
                                        onToggleDetails={startTrackingEtlProgress}
                                        showItemPreview={showItemPreview}
                                    />
                                ))}
                            </div>
                        )}
                        {olapEtls.length > 0 && (
                            <div key="olap-etls" data-testid="olap-etls">
                                <HrHeader className="olap-etl" count={olapEtls.length}>
                                    <Icon icon="olap-etl" />
                                    OLAP ETL
                                </HrHeader>

                                {olapEtls.map((x) => (
                                    <OlapEtlPanel
                                        {...sharedPanelProps}
                                        key={taskKey(x.shared)}
                                        data={x}
                                        onToggleDetails={startTrackingEtlProgress}
                                        showItemPreview={showItemPreview}
                                    />
                                ))}
                            </div>
                        )}
                        {kafkaEtls.length > 0 && (
                            <div key="kafka-etls" data-testid="kafka-etls">
                                <HrHeader className="kafka-etl" count={kafkaEtls.length}>
                                    <Icon icon="kafka-etl" />
                                    KAFKA ETL
                                </HrHeader>

                                {kafkaEtls.map((x) => (
                                    <KafkaEtlPanel
                                        {...sharedPanelProps}
                                        key={taskKey(x.shared)}
                                        data={x}
                                        onToggleDetails={startTrackingEtlProgress}
                                        showItemPreview={showItemPreview}
                                    />
                                ))}
                            </div>
                        )}
                        {rabbitMqEtls.length > 0 && (
                            <div key="rabbitmq-etls" data-testid="rabbitmq-etls">
                                <HrHeader className="rabbitmq-etl" count={rabbitMqEtls.length}>
                                    <Icon icon="rabbitmq-etl" />
                                    RABBITMQ ETL
                                </HrHeader>

                                {rabbitMqEtls.map((x) => (
                                    <RabbitMqEtlPanel
                                        {...sharedPanelProps}
                                        key={taskKey(x.shared)}
                                        data={x}
                                        onToggleDetails={startTrackingEtlProgress}
                                        showItemPreview={showItemPreview}
                                    />
                                ))}
                            </div>
                        )}
                        {azureQueueStorageEtls.length > 0 && (
                            <div key="azure-queue-storage-etls" data-testid="azure-queue-storage-etls">
                                <HrHeader className="azure-queue-storage-etl" count={azureQueueStorageEtls.length}>
                                    <Icon icon="azure-queue-storage-etl" />
                                    AZURE QUEUE STORAGE ETL
                                </HrHeader>

                                {azureQueueStorageEtls.map((x) => (
                                    <AzureQueueStorageEtlPanel
                                        {...sharedPanelProps}
                                        key={taskKey(x.shared)}
                                        data={x}
                                        onToggleDetails={startTrackingEtlProgress}
                                        showItemPreview={showItemPreview}
                                    />
                                ))}
                            </div>
                        )}
                        {amazonSqsEtls.length > 0 && (
                            <div key="amazon-sqs-etls">
                                <HrHeader className="amazon-sqs-etl" count={amazonSqsEtls.length}>
                                    <Icon icon="amazon-sqs-etl" />
                                    AMAZON SQS ETL
                                </HrHeader>

                                {amazonSqsEtls.map((x) => (
                                    <AmazonSqsEtlPanel
                                        {...sharedPanelProps}
                                        key={taskKey(x.shared)}
                                        data={x}
                                        onToggleDetails={startTrackingEtlProgress}
                                        showItemPreview={showItemPreview}
                                    />
                                ))}
                            </div>
                        )}
                        {kafkaSinks.length > 0 && (
                            <div key="kafka-sinks" data-testid="kafka-sinks">
                                <HrHeader className="kafka-sink" count={kafkaSinks.length}>
                                    <Icon icon="kafka-sink" />
                                    KAFKA SINK
                                </HrHeader>

                                {kafkaSinks.map((x) => (
                                    <KafkaSinkPanel {...sharedPanelProps} key={taskKey(x.shared)} data={x} />
                                ))}
                            </div>
                        )}
                        {rabbitMqSinks.length > 0 && (
                            <div key="rabbitmq-sinks" data-testid="rabbitmq-sinks">
                                <HrHeader className="rabbitmq-sink" count={rabbitMqSinks.length}>
                                    <Icon icon="rabbitmq-sink" />
                                    RABBITMQ SINK
                                </HrHeader>

                                {rabbitMqSinks.map((x) => (
                                    <RabbitMqSinkPanel {...sharedPanelProps} key={taskKey(x.shared)} data={x} />
                                ))}
                            </div>
                        )}
                        {elasticSearchEtls.length > 0 && (
                            <div key="elastic-search-etls" data-testid="elastic-search-etls">
                                <HrHeader className="elastic-etl" count={elasticSearchEtls.length}>
                                    <Icon icon="elastic-search-etl" />
                                    Elasticsearch ETL
                                </HrHeader>

                                {elasticSearchEtls.map((x) => (
                                    <ElasticSearchEtlPanel
                                        {...sharedPanelProps}
                                        key={taskKey(x.shared)}
                                        data={x}
                                        onToggleDetails={startTrackingEtlProgress}
                                        showItemPreview={showItemPreview}
                                    />
                                ))}
                            </div>
                        )}
                        {backups.length > 0 && (
                            <div key="backups" data-testid="backups">
                                <HrHeader className="periodic-backup" count={backups.length}>
                                    <Icon icon="backup" />
                                    Periodic Backup
                                </HrHeader>

                                {backups.map((x) => (
                                    <PeriodicBackupPanel
                                        sourceView="OngoingTasks"
                                        forceReload={reload}
                                        allowSelect
                                        {...sharedPanelProps}
                                        key={taskKey(x.shared)}
                                        data={x}
                                    />
                                ))}
                            </div>
                        )}
                        {subscriptionsDatabaseCount > 0 && (
                            <div key="subscriptions" data-testid="subscriptions">
                                <HrHeader
                                    className="subscription"
                                    count={
                                        subscriptionsDatabaseLimitStatus === "notReached"
                                            ? subscriptionsDatabaseCount
                                            : null
                                    }
                                >
                                    <Icon icon="subscription" />
                                    Subscription
                                    {subscriptionsDatabaseLimitStatus !== "notReached" && (
                                        <CounterBadge
                                            count={subscriptionsDatabaseCount}
                                            limit={subscriptionsDatabaseLimit}
                                            className="ms-3"
                                        />
                                    )}
                                </HrHeader>

                                {subscriptions.map((x) => {
                                    const connectionDetails = tasks.subscriptionConnectionDetails.find(
                                        (details) => x.shared.taskId === details.SubscriptionId
                                    );

                                    return (
                                        <SubscriptionPanel
                                            {...sharedPanelProps}
                                            connections={connectionDetails}
                                            dropSubscription={(workerId) =>
                                                dropSubscription(
                                                    x.shared.taskId,
                                                    x.shared.taskName,
                                                    x.shared.responsibleNodeTag,
                                                    workerId
                                                )
                                            }
                                            onToggleDetails={async (newState) => {
                                                if (newState) {
                                                    await refreshSubscriptionInfo(x.shared.taskId, x.shared.taskName);
                                                }
                                            }}
                                            refreshSubscriptionInfo={() =>
                                                refreshSubscriptionInfo(x.shared.taskId, x.shared.taskName)
                                            }
                                            key={taskKey(x.shared)}
                                            data={x}
                                        />
                                    );
                                })}
                            </div>
                        )}
                        {hubDefinitions.length > 0 && (
                            <div key="replication-hubs" data-testid="replication-hubs">
                                <HrHeader className="pull-replication-hub" count={hubDefinitions.length}>
                                    <Icon icon="pull-replication-hub" />
                                    Replication Hub
                                </HrHeader>

                                {hubDefinitions.map((def) => (
                                    <ReplicationHubDefinitionPanel
                                        {...sharedPanelProps}
                                        key={taskKey(def.shared)}
                                        data={def}
                                        onToggleDetails={startTrackingReplicationProgress}
                                        connectedSinks={replicationHubs.filter(
                                            (x) => x.shared.taskId === def.shared.taskId
                                        )}
                                    />
                                ))}
                            </div>
                        )}
                        {replicationSinks.length > 0 && (
                            <div key="replication-sinks" data-testid="replication-sinks">
                                <HrHeader className="pull-replication-sink" count={replicationSinks.length}>
                                    <Icon icon="pull-replication-agent" />
                                    Replication Sink
                                </HrHeader>

                                {replicationSinks.map((x) => (
                                    <ReplicationSinkPanel
                                        {...sharedPanelProps}
                                        key={taskKey(x.shared)}
                                        onToggleDetails={startTrackingReplicationProgress}
                                        data={x}
                                    />
                                ))}
                            </div>
                        )}
                    </div>
                </div>
            </Row>
            <div id="modalContainer" className="bs5" />
        </div>
    );
}

function nameMatch(taskName: string, searchText: string): boolean {
    return taskName.toLowerCase().includes(searchText.toLowerCase());
}

function filterOngoingTask(sharedInfo: OngoingTaskSharedInfo, filter: OngoingTasksFilterCriteria) {
    const isTaskNameMatching = nameMatch(sharedInfo.taskName, filter.searchText);

    if (!isTaskNameMatching) {
        return false;
    }

    if (filter.types.length === 0) {
        return true;
    }

    const isReplicationTypeMatching =
        filter.types.includes("Replication") &&
        (sharedInfo.taskType === "Replication" ||
            sharedInfo.taskType === "PullReplicationAsHub" ||
            sharedInfo.taskType === "PullReplicationAsSink");

    const isETLTypeMatching = filter.types.includes("ETL") && sharedInfo.taskType.endsWith("Etl");

    const isSinkTypeMatching =
        filter.types.includes("Sink") &&
        (sharedInfo.taskType === "KafkaQueueSink" || sharedInfo.taskType === "RabbitQueueSink");

    const isBackupTypeMatching = filter.types.includes("Backup") && sharedInfo.taskType === "Backup";

    const isSubscriptionTypeMatching = filter.types.includes("Subscription") && sharedInfo.taskType === "Subscription";

    return (
        isReplicationTypeMatching ||
        isETLTypeMatching ||
        isSinkTypeMatching ||
        isBackupTypeMatching ||
        isSubscriptionTypeMatching
    );
}

function getFilteredTasks(state: OngoingTasksState, filter: OngoingTasksFilterCriteria) {
    const filteredTasks = state.tasks.filter((x) => filterOngoingTask(x.shared, filter));

    const internalReplicationVisible =
        (filter.types.length === 0 || filter.types.includes("Replication")) &&
        nameMatch("Internal Replication", filter.searchText);

    const filteredReplications = internalReplicationVisible ? state.internalReplication : [];

    return {
        internalReplications: filteredReplications,
        externalReplications: filteredTasks.filter(
            (x) => x.shared.taskType === "Replication"
        ) as OngoingTaskExternalReplicationInfo[],
        ravenEtls: filteredTasks.filter((x) => x.shared.taskType === "RavenEtl") as OngoingTaskRavenEtlInfo[],
        sqlEtls: filteredTasks.filter((x) => x.shared.taskType === "SqlEtl") as OngoingTaskSqlEtlInfo[],
        snowflakeEtls: filteredTasks.filter(
            (x) => x.shared.taskType === "SnowflakeEtl"
        ) as OngoingTaskSnowflakeEtlInfo[],
        olapEtls: filteredTasks.filter((x) => x.shared.taskType === "OlapEtl") as OngoingTaskOlapEtlInfo[],
        kafkaEtls: filteredTasks.filter((x) => x.shared.taskType === "KafkaQueueEtl") as OngoingTaskKafkaEtlInfo[],
        rabbitMqEtls: filteredTasks.filter(
            (x) => x.shared.taskType === "RabbitQueueEtl"
        ) as OngoingTaskRabbitMqEtlInfo[],
        azureQueueStorageEtls: filteredTasks.filter(
            (x) => x.shared.taskType === "AzureQueueStorageQueueEtl"
        ) as OngoingTaskAzureQueueStorageEtlInfo[],
        amazonSqsEtls: filteredTasks.filter(
            (x) => x.shared.taskType === "AmazonSqsQueueEtl"
        ) as OngoingTaskAmazonSqsEtlInfo[],
        kafkaSinks: filteredTasks.filter((x) => x.shared.taskType === "KafkaQueueSink") as OngoingTaskKafkaSinkInfo[],
        rabbitMqSinks: filteredTasks.filter(
            (x) => x.shared.taskType === "RabbitQueueSink"
        ) as OngoingTaskRabbitMqSinkInfo[],
        elasticSearchEtls: filteredTasks.filter(
            (x) => x.shared.taskType === "ElasticSearchEtl"
        ) as OngoingTaskElasticSearchEtlInfo[],
        backups: filteredTasks.filter((x) => x.shared.taskType === "Backup") as OngoingTaskPeriodicBackupInfo[],
        replicationHubs: filteredTasks.filter(
            (x) => x.shared.taskType === "PullReplicationAsHub"
        ) as OngoingTaskReplicationHubInfo[],
        replicationSinks: filteredTasks.filter(
            (x) => x.shared.taskType === "PullReplicationAsSink"
        ) as OngoingTaskReplicationSinkInfo[],
        subscriptions: state.subscriptions.filter((x) => filterOngoingTask(x.shared, filter)),
        hubDefinitions: state.replicationHubs.filter((x) => filterOngoingTask(x.shared, filter)),
    };
}

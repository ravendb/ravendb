import React, { useCallback, useEffect, useReducer, useState } from "react";
import { useServices } from "hooks/useServices";
import { OngoingTasksState, ongoingTasksReducer, ongoingTasksReducerInitializer } from "./OngoingTasksReducer";
import appUrl from "common/appUrl";
import { ExternalReplicationPanel } from "./panels/ExternalReplicationPanel";
import {
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
import { OngoingTaskProgressProvider } from "./OngoingTaskProgressProvider";
import { BaseOngoingTaskPanelProps, taskKey, useOngoingTasksOperations } from "../shared/shared";
import EtlTaskProgress = Raven.Server.Documents.ETL.Stats.EtlTaskProgress;
import "./OngoingTaskPage.scss";
import etlScriptDefinitionCache from "models/database/stats/etlScriptDefinitionCache";
import TaskUtils from "../../../../utils/TaskUtils";
import { KafkaEtlPanel } from "./panels/KafkaEtlPanel";
import { RabbitMqEtlPanel } from "./panels/RabbitMqEtlPanel";
import useInterval from "hooks/useInterval";
import { Alert, Button } from "reactstrap";
import { HrHeader } from "components/common/HrHeader";
import { EmptySet } from "components/common/EmptySet";
import { Icon } from "components/common/Icon";
import OngoingTasksFilter, { OngoingTaskFilterType, OngoingTasksFilterCriteria } from "./OngoingTasksFilter";
import { exhaustiveStringTuple } from "components/utils/common";
import { InputItem } from "components/models/common";
import assertUnreachable from "components/utils/assertUnreachable";
import OngoingTaskSelectActions from "./OngoingTaskSelectActions";
import OngoingTaskOperationConfirm from "../shared/OngoingTaskOperationConfirm";
import { StickyHeader } from "components/common/StickyHeader";
import { KafkaSinkPanel } from "components/pages/database/tasks/ongoingTasks/panels/KafkaSinkPanel";
import { RabbitMqSinkPanel } from "components/pages/database/tasks/ongoingTasks/panels/RabbitMqSinkPanel";
import { CounterBadge } from "components/common/CounterBadge";
import { getLicenseLimitReachStatus } from "components/utils/licenseLimitsUtils";
import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import { FlexGrow } from "components/common/FlexGrow";
import OngoingTaskAddModal from "./OngoingTaskAddModal";
import { useAppSelector } from "components/store";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useRavenLink } from "components/hooks/useRavenLink";
import { throttledUpdateLicenseLimitsUsage } from "components/common/shell/setup";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { accessManagerSelectors } from "components/common/shell/accessManagerSlice";
import { compareSets } from "common/typeUtils";

export function OngoingTasksPage() {
    const db = useAppSelector(databaseSelectors.activeDatabase);
    const isClusterAdminOrClusterNode = useAppSelector(accessManagerSelectors.isClusterAdminOrClusterNode);
    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.hasDatabaseAdminAccess());
    const hasDatabaseWriteAccess = useAppSelector(accessManagerSelectors.hasDatabaseWriteAccess());

    const { tasksService } = useServices();
    const [tasks, dispatch] = useReducer(ongoingTasksReducer, db, ongoingTasksReducerInitializer);

    const { value: isNewTaskModalOpen, toggle: toggleIsNewTaskModalOpen } = useBoolean(false);
    const { value: progressEnabled, setTrue: startTrackingProgress } = useBoolean(false);
    const [definitionCache] = useState(() => new etlScriptDefinitionCache(db.name));
    const [filter, setFilter] = useState<OngoingTasksFilterCriteria>({
        searchText: "",
        types: [],
    });

    const upgradeLicenseLink = useRavenLink({ hash: "FLDLO4", isDocs: false });
    const ongoingTasksDocsLink = useRavenLink({ hash: "K4ZTNA" });

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
                dispatch({
                    type: "TasksLoadError",
                    location,
                    error: e,
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
                type: "ProgressLoaded",
                progress,
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

    const serverWideTasksUrl = appUrl.forServerWideTasks();

    const filteredTasks = getFilteredTasks(tasks, filter);

    const {
        externalReplications,
        ravenEtls,
        sqlEtls,
        olapEtls,
        kafkaEtls,
        rabbitMqEtls,
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

    const getSelectedTaskShardedInfos = () =>
        [...tasks.tasks, ...tasks.subscriptions, ...tasks.replicationHubs]
            .filter((x) => selectedTaskIds.includes(x.shared.taskId))
            .map((x) => x.shared);

    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    const { replicationHubs: ignored, ...filteredWithoutReplicationHubs } = filteredTasks;
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
        tasks.subscriptions.length;

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

        const taskInfo = await Promise.all(loadTasks);

        const targetNode = taskInfo.find((x) => x.ResponsibleNode.NodeTag);

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

    const {
        onTaskOperation,
        operationConfirm,
        cancelOperationConfirm,
        isTogglingState,
        isDeleting,
        isTogglingStateAny,
        isDeletingAny,
    } = useOngoingTasksOperations(reload);

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

    return (
        <div className="content-margin">
            {subscriptionsClusterLimitStatus !== "notReached" && (
                <Alert
                    color={subscriptionsClusterLimitStatus === "limitReached" ? "danger" : "warning"}
                    className="text-center mb-3"
                >
                    <Icon icon="cluster" />
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
                </Alert>
            )}

            {subscriptionsDatabaseLimitStatus !== "notReached" && (
                <Alert
                    color={subscriptionsDatabaseLimitStatus === "limitReached" ? "danger" : "warning"}
                    className="text-center mb-3"
                >
                    <Icon icon="database" />
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
                </Alert>
            )}

            {progressEnabled && <OngoingTaskProgressProvider onEtlProgress={onEtlProgress} />}
            {operationConfirm && <OngoingTaskOperationConfirm {...operationConfirm} toggle={cancelOperationConfirm} />}
            <StickyHeader>
                <div className="hstack gap-3 flex-wrap">
                    {hasDatabaseWriteAccess && (
                        <>
                            {isNewTaskModalOpen && (
                                <OngoingTaskAddModal
                                    toggle={toggleIsNewTaskModalOpen}
                                    subscriptionsDatabaseCount={subscriptionsDatabaseCount}
                                />
                            )}
                            <div id="NewTaskButton">
                                <Button onClick={toggleIsNewTaskModalOpen} color="primary" className="rounded-pill">
                                    <Icon icon="ongoing-tasks" addon="plus" />
                                    Add a Database Task
                                </Button>
                            </div>
                        </>
                    )}

                    <FlexGrow />

                    {isClusterAdminOrClusterNode && (
                        <Button
                            color="link"
                            size="sm"
                            target="_blank"
                            href={serverWideTasksUrl}
                            title="Go to the Server-Wide Tasks view"
                        >
                            <Icon icon="server-wide-tasks" />
                            Server-Wide Tasks
                        </Button>
                    )}

                    <AboutViewFloating>
                        <AccordionItemWrapper
                            icon="about"
                            color="info"
                            heading="About this view"
                            description="Get additional info on this feature"
                            targetId="about-view"
                        >
                            <div>
                                <strong>Ongoing-tasks</strong> are work tasks assigned to the database.
                                <ul className="margin-top-xxs">
                                    <li>
                                        A few examples are: <br />
                                        Executing a periodic backup of the database, replicating to another RavenDB
                                        instance, or transferring data to external frameworks such as Kafaka, RabbitMQ,
                                        etc.
                                    </li>
                                    <li className="margin-top-xxs">
                                        Click the &quot;Add a Database Task&quot; button to view all available tasks and
                                        select from the list.
                                    </li>
                                </ul>
                            </div>
                            <div>
                                <strong>Running in the background</strong>, each ongoing task is handled by a designated
                                node from the Database-Group nodes.
                                <ul className="margin-top-xxs">
                                    <li>
                                        For each task, you can specify which node will be responsible for the task and
                                        whether the cluster may assign a different node when that node is down.
                                    </li>
                                    <li className="margin-top-xxs">
                                        If not specified, the cluster will decide which node will handle the task.
                                    </li>
                                </ul>
                            </div>
                            <hr />
                            <div className="small-label mb-2">useful links</div>
                            <a href={ongoingTasksDocsLink} target="_blank">
                                <Icon icon="newtab" /> Docs - Ongoing Tasks
                            </a>
                        </AccordionItemWrapper>
                    </AboutViewFloating>
                </div>

                {allTasksCount > 0 && (
                    <div className="mt-3">
                        <OngoingTasksFilter
                            filter={filter}
                            setFilter={setFilter}
                            filterByStatusOptions={getFilterByStatusOptions(tasks)}
                            tasksCount={allTasksCount}
                        />
                    </div>
                )}

                {allTasksCount > 0 && hasDatabaseAdminAccess && (
                    <OngoingTaskSelectActions
                        allTasks={filteredDatabaseTaskIds}
                        selectedTasks={selectedTaskIds}
                        setSelectedTasks={setSelectedTaskIds}
                        onTaskOperation={(type) => onTaskOperation(type, getSelectedTaskShardedInfos())}
                        isTogglingState={isTogglingStateAny}
                        isDeleting={isDeletingAny}
                    />
                )}
            </StickyHeader>
            <div className="flex-vertical">
                <div className="scroll flex-grow">
                    {allTasksCount === 0 && <EmptySet>No tasks have been created for this Database Group.</EmptySet>}

                    {externalReplications.length > 0 && (
                        <div key="external-replications">
                            <HrHeader className="external-replication" count={externalReplications.length}>
                                <Icon icon="external-replication" /> External Replication
                            </HrHeader>

                            {externalReplications.map((x) => (
                                <ExternalReplicationPanel {...sharedPanelProps} key={taskKey(x.shared)} data={x} />
                            ))}
                        </div>
                    )}

                    {ravenEtls.length > 0 && (
                        <div key="raven-etls">
                            <HrHeader className="ravendb-etl" count={ravenEtls.length}>
                                <Icon icon="etl" />
                                RavenDB ETL
                            </HrHeader>

                            {ravenEtls.map((x) => (
                                <RavenEtlPanel
                                    {...sharedPanelProps}
                                    key={taskKey(x.shared)}
                                    data={x}
                                    onToggleDetails={startTrackingProgress}
                                    showItemPreview={showItemPreview}
                                />
                            ))}
                        </div>
                    )}

                    {sqlEtls.length > 0 && (
                        <div key="sql-etls">
                            <HrHeader className="sql-etl" count={sqlEtls.length}>
                                <Icon icon="sql-etl" />
                                SQL ETL
                            </HrHeader>

                            {sqlEtls.map((x) => (
                                <SqlEtlPanel
                                    {...sharedPanelProps}
                                    key={taskKey(x.shared)}
                                    data={x}
                                    onToggleDetails={startTrackingProgress}
                                    showItemPreview={showItemPreview}
                                />
                            ))}
                        </div>
                    )}

                    {olapEtls.length > 0 && (
                        <div key="olap-etls">
                            <HrHeader className="olap-etl" count={olapEtls.length}>
                                <Icon icon="olap-etl" />
                                OLAP ETL
                            </HrHeader>

                            {olapEtls.map((x) => (
                                <OlapEtlPanel
                                    {...sharedPanelProps}
                                    key={taskKey(x.shared)}
                                    data={x}
                                    onToggleDetails={startTrackingProgress}
                                    showItemPreview={showItemPreview}
                                />
                            ))}
                        </div>
                    )}

                    {kafkaEtls.length > 0 && (
                        <div key="kafka-etls">
                            <HrHeader className="kafka-etl" count={kafkaEtls.length}>
                                <Icon icon="kafka-etl" />
                                KAFKA ETL
                            </HrHeader>

                            {kafkaEtls.map((x) => (
                                <KafkaEtlPanel
                                    {...sharedPanelProps}
                                    key={taskKey(x.shared)}
                                    data={x}
                                    onToggleDetails={startTrackingProgress}
                                    showItemPreview={showItemPreview}
                                />
                            ))}
                        </div>
                    )}

                    {rabbitMqEtls.length > 0 && (
                        <div key="rabbitmq-etls">
                            <HrHeader className="rabbitmq-etl" count={rabbitMqEtls.length}>
                                <Icon icon="rabbitmq-etl" />
                                RABBITMQ ETL
                            </HrHeader>

                            {rabbitMqEtls.map((x) => (
                                <RabbitMqEtlPanel
                                    {...sharedPanelProps}
                                    key={taskKey(x.shared)}
                                    data={x}
                                    onToggleDetails={startTrackingProgress}
                                    showItemPreview={showItemPreview}
                                />
                            ))}
                        </div>
                    )}

                    {kafkaSinks.length > 0 && (
                        <div key="kafka-sinks">
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
                        <div key="rabbitmq-sinks">
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
                        <div key="elastic-search-etls">
                            <HrHeader className="elastic-etl" count={elasticSearchEtls.length}>
                                <Icon icon="elastic-search-etl" />
                                Elasticsearch ETL
                            </HrHeader>

                            {elasticSearchEtls.map((x) => (
                                <ElasticSearchEtlPanel
                                    {...sharedPanelProps}
                                    key={taskKey(x.shared)}
                                    data={x}
                                    onToggleDetails={startTrackingProgress}
                                    showItemPreview={showItemPreview}
                                />
                            ))}
                        </div>
                    )}

                    {backups.length > 0 && (
                        <div key="backups">
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
                        <div key="subscriptions">
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
                        <div key="replication-hubs">
                            <HrHeader className="pull-replication-hub" count={hubDefinitions.length}>
                                <Icon icon="pull-replication-hub" />
                                Replication Hub
                            </HrHeader>

                            {hubDefinitions.map((def) => (
                                <ReplicationHubDefinitionPanel
                                    {...sharedPanelProps}
                                    key={taskKey(def.shared)}
                                    data={def}
                                    connectedSinks={replicationHubs.filter(
                                        (x) => x.shared.taskId === def.shared.taskId
                                    )}
                                />
                            ))}
                        </div>
                    )}

                    {replicationSinks.length > 0 && (
                        <div key="replication-sinks">
                            <HrHeader className="pull-replication-sink" count={replicationSinks.length}>
                                <Icon icon="pull-replication-agent" />
                                Replication Sink
                            </HrHeader>

                            {replicationSinks.map((x) => (
                                <ReplicationSinkPanel {...sharedPanelProps} key={taskKey(x.shared)} data={x} />
                            ))}
                        </div>
                    )}
                </div>
            </div>
            <div id="modalContainer" className="bs5" />
        </div>
    );
}

function getFilterByStatusOptions(state: OngoingTasksState): InputItem<OngoingTaskFilterType>[] {
    const backupCount = state.tasks.filter((x) => x.shared.taskType === "Backup").length;
    const subscriptionCount = state.subscriptions.length;

    const etlCount = state.tasks.filter((x) => x.shared.taskType.endsWith("Etl")).length;

    const sinkCount = state.tasks.filter(
        (x) => x.shared.taskType === "KafkaQueueSink" || x.shared.taskType === "RabbitQueueSink"
    ).length;

    const replicationHubCount = state.replicationHubs.length;
    const replicationSinkCount = state.tasks.filter((x) => x.shared.taskType === "PullReplicationAsSink").length;
    const externalReplicationCount = state.tasks.filter((x) => x.shared.taskType === "Replication").length;
    const replicationCount = externalReplicationCount + replicationHubCount + replicationSinkCount;

    return exhaustiveStringTuple<OngoingTaskFilterType>()("Replication", "ETL", "Sink", "Backup", "Subscription").map(
        (filterType) => {
            switch (filterType) {
                case "Replication":
                    return {
                        label: filterType,
                        value: filterType,
                        count: replicationCount,
                    };
                case "ETL":
                    return { label: filterType, value: filterType, count: etlCount };
                case "Sink":
                    return { label: filterType, value: filterType, count: sinkCount };
                case "Backup":
                    return { label: filterType, value: filterType, count: backupCount };
                case "Subscription":
                    return { label: filterType, value: filterType, count: subscriptionCount };
                default:
                    assertUnreachable(filterType);
            }
        }
    );
}

function filterOngoingTask(sharedInfo: OngoingTaskSharedInfo, filter: OngoingTasksFilterCriteria) {
    const isTaskNameMatching = sharedInfo.taskName.toLowerCase().includes(filter.searchText.toLowerCase());

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

    return {
        externalReplications: filteredTasks.filter(
            (x) => x.shared.taskType === "Replication"
        ) as OngoingTaskExternalReplicationInfo[],
        ravenEtls: filteredTasks.filter((x) => x.shared.taskType === "RavenEtl") as OngoingTaskRavenEtlInfo[],
        sqlEtls: filteredTasks.filter((x) => x.shared.taskType === "SqlEtl") as OngoingTaskSqlEtlInfo[],
        olapEtls: filteredTasks.filter((x) => x.shared.taskType === "OlapEtl") as OngoingTaskOlapEtlInfo[],
        kafkaEtls: filteredTasks.filter((x) => x.shared.taskType === "KafkaQueueEtl") as OngoingTaskKafkaEtlInfo[],
        rabbitMqEtls: filteredTasks.filter(
            (x) => x.shared.taskType === "RabbitQueueEtl"
        ) as OngoingTaskRabbitMqEtlInfo[],
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

import database from "models/resources/database";
import React, { useCallback, useEffect, useReducer, useState } from "react";
import { useServices } from "hooks/useServices";
import { OngoingTasksState, ongoingTasksReducer, ongoingTasksReducerInitializer } from "./OngoingTasksReducer";
import { useAccessManager } from "hooks/useAccessManager";
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
import AccordionLicenseLimited from "components/common/AccordionLicenseLimited";

interface OngoingTasksPageProps {
    database: database;
}

export function OngoingTasksPage(props: OngoingTasksPageProps) {
    const { database } = props;

    const { canReadWriteDatabase, isClusterAdminOrClusterNode, isAdminAccessOrAbove } = useAccessManager();
    const { tasksService } = useServices();
    const [tasks, dispatch] = useReducer(ongoingTasksReducer, database, ongoingTasksReducerInitializer);

    const { value: isNewTaskModalOpen, toggle: toggleIsNewTaskModalOpen } = useBoolean(false);
    const { value: progressEnabled, setTrue: startTrackingProgress } = useBoolean(false);
    const [definitionCache] = useState(() => new etlScriptDefinitionCache(database));
    const [filter, setFilter] = useState<OngoingTasksFilterCriteria>({
        searchText: "",
        types: [],
    });

    const fetchTasks = useCallback(
        async (location: databaseLocationSpecifier) => {
            try {
                const tasks = await tasksService.getOngoingTasks(database, location);
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
        [database, tasksService, dispatch]
    );

    const reload = useCallback(async () => {
        // if database is sharded we need to load from both orchestrator and target node point of view
        // in case of non-sharded - we have single level: node

        if (database.isSharded()) {
            const orchestratorTasks = database.nodes().map((node) => fetchTasks({ nodeTag: node.tag }));
            await Promise.all(orchestratorTasks);
        }

        const loadTasks = tasks.locations.map(fetchTasks);
        await Promise.all(loadTasks);
    }, [tasks, fetchTasks, database]);

    useInterval(reload, 10_000);

    useEffect(() => {
        reload();
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [database]);

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

    const canNavigateToServerWideTasks = isClusterAdminOrClusterNode();
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

    const getSelectedTaskShardedInfos = () =>
        [...tasks.tasks, ...tasks.subscriptions, ...tasks.replicationHubs]
            .filter((x) => selectedTaskNames.includes(x.shared.taskName))
            .map((x) => x.shared);

    const filteredDatabaseTaskNames = Object.values(_.omit(filteredTasks, ["replicationHubs"]))
        .flat()
        .filter((x) => !x.shared.serverWide)
        .map((x) => x.shared.taskName);

    const [selectedTaskNames, setSelectedTaskNames] = useState<string[]>(filteredDatabaseTaskNames);

    useEffect(() => {
        const updatedSelectedTaskNames = selectedTaskNames.filter((name) => filteredDatabaseTaskNames.includes(name));

        if (!_.isEqual(updatedSelectedTaskNames, selectedTaskNames)) {
            setSelectedTaskNames(updatedSelectedTaskNames);
        }
    }, [filteredDatabaseTaskNames, selectedTaskNames]);

    const allTasksCount =
        tasks.tasks.filter((x) => x.shared.taskType !== "PullReplicationAsHub").length +
        tasks.replicationHubs.length +
        tasks.subscriptions.length;

    const refreshSubscriptionInfo = async (taskId: number, taskName: string) => {
        const loadTasks = database.nodes().map(async (nodeInfo) => {
            const nodeTag = nodeInfo.tag;
            const task = await tasksService.getSubscriptionTaskInfo(database, taskId, taskName, nodeTag);

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
                database,
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
        await tasksService.dropSubscription(database, taskId, taskName, nodeTag, workerId);
    };

    const {
        onTaskOperation,
        operationConfirm,
        cancelOperationConfirm,
        isTogglingState,
        isDeleting,
        isTogglingStateAny,
        isDeletingAny,
    } = useOngoingTasksOperations(database, reload);

    const sharedPanelProps: Omit<BaseOngoingTaskPanelProps<OngoingTaskInfo>, "data"> = {
        db: database,
        onTaskOperation,
        isSelected: (taskName: string) => selectedTaskNames.includes(taskName),
        toggleSelection: (checked: boolean, taskShardedInfo: OngoingTaskSharedInfo) => {
            if (checked) {
                setSelectedTaskNames((selectedNames) => [...selectedNames, taskShardedInfo.taskName]);
            } else {
                setSelectedTaskNames((selectedNames) => selectedNames.filter((x) => x !== taskShardedInfo.taskName));
            }
        },
        isTogglingState,
        isDeleting,
    };

    const isCommunity = useAppSelector(licenseSelectors.licenseType) === "Community";

    // TODO get form endpoint
    const subscriptionsServerCount = 0;

    const subscriptionsDatabaseCount = subscriptions.length;

    // TODO get from license selector
    const subscriptionsServerLimit = 3 * 5;
    const subscriptionsDatabaseLimit = 3;

    const subscriptionsServerLimitStatus = getLicenseLimitReachStatus(
        subscriptionsServerCount,
        subscriptionsServerLimit
    );

    const subscriptionsDatabaseLimitStatus = getLicenseLimitReachStatus(
        subscriptionsDatabaseCount,
        subscriptionsDatabaseLimit
    );

    return (
        <div>
            {isCommunity && (
                <>
                    {subscriptionsServerLimitStatus !== "notReached" && (
                        <Alert
                            color={subscriptionsServerLimitStatus === "limitReached" ? "danger" : "warning"}
                            className="text-center"
                        >
                            Your server {subscriptionsServerLimitStatus === "limitReached" ? "reached" : "is reaching"}{" "}
                            the <strong>maximum number of subscriptions</strong> allowed by your license{" "}
                            <strong>
                                ({subscriptionsServerCount}/{subscriptionsServerLimit})
                            </strong>
                            <br />
                            <strong>
                                <a href="https://ravendb.net/l/FLDLO4" target="_blank">
                                    Upgrade your license
                                </a>{" "}
                            </strong>
                            to add more
                        </Alert>
                    )}

                    {subscriptionsDatabaseLimitStatus !== "notReached" && (
                        <Alert
                            color={subscriptionsDatabaseLimitStatus === "limitReached" ? "danger" : "warning"}
                            className="text-center"
                        >
                            Your database{" "}
                            {subscriptionsDatabaseLimitStatus === "limitReached" ? "reached" : "is reaching"} the{" "}
                            <strong>maximum number of subscriptions</strong> allowed by your license{" "}
                            <strong>
                                ({subscriptionsDatabaseCount}/{subscriptionsDatabaseLimit})
                            </strong>
                            <br />
                            <strong>
                                <a href="https://ravendb.net/l/FLDLO4" target="_blank">
                                    Upgrade your license
                                </a>{" "}
                            </strong>
                            to add more
                        </Alert>
                    )}
                </>
            )}

            {progressEnabled && <OngoingTaskProgressProvider db={database} onEtlProgress={onEtlProgress} />}
            {operationConfirm && <OngoingTaskOperationConfirm {...operationConfirm} toggle={cancelOperationConfirm} />}
            <StickyHeader>
                <div className="hstack gap-3 flex-wrap">
                    {canReadWriteDatabase(database) && (
                        <>
                            {isNewTaskModalOpen && (
                                <OngoingTaskAddModal
                                    db={database}
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

                    {canNavigateToServerWideTasks && (
                        <Button
                            color="link"
                            size="sm"
                            outline
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
                            icon="ongoing-tasks"
                            color="info"
                            heading="About this view"
                            description="Get additional info on what this feature can offer you"
                            targetId="about-view"
                        >
                            <p>
                                This is <strong>Ongoing Tasks</strong> view.
                            </p>
                        </AccordionItemWrapper>
                        {isCommunity && (
                            <AccordionLicenseLimited
                                targetId="license-limit"
                                description="Unleash the full potential and upgrade your plan."
                                featureName="Ongoing Tasks"
                                featureIcon="ongoing-tasks"
                            />
                        )}
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

                {allTasksCount > 0 && isAdminAccessOrAbove(database) && (
                    <OngoingTaskSelectActions
                        allTasks={filteredDatabaseTaskNames}
                        selectedTasks={selectedTaskNames}
                        setSelectedTasks={setSelectedTaskNames}
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
                            <HrHeader className="subscription" count={!isCommunity ? subscriptionsDatabaseCount : null}>
                                <Icon icon="subscription" />
                                Subscription
                                {isCommunity && (
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

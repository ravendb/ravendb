import database from "models/resources/database";
import React, { useCallback, useEffect, useReducer, useState } from "react";
import { useServices } from "hooks/useServices";
import { OngoingTasksState, ongoingTasksReducer, ongoingTasksReducerInitializer } from "./OngoingTasksReducer";
import { useAccessManager } from "hooks/useAccessManager";
import createOngoingTask from "viewmodels/database/tasks/createOngoingTask";
import app from "durandal/app";
import appUrl from "common/appUrl";
import { ExternalReplicationPanel } from "./panels/ExternalReplicationPanel";
import {
    OngoingTaskElasticSearchEtlInfo,
    OngoingTaskExternalReplicationInfo,
    OngoingTaskInfo,
    OngoingTaskKafkaEtlInfo,
    OngoingTaskOlapEtlInfo,
    OngoingTaskPeriodicBackupInfo,
    OngoingTaskRabbitMqEtlInfo,
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
import { BaseOngoingTaskPanelProps, taskKey, useOngoingTasksOperations } from "../shared";
import EtlTaskProgress = Raven.Server.Documents.ETL.Stats.EtlTaskProgress;
import "./OngoingTaskPage.scss";
import etlScriptDefinitionCache from "models/database/stats/etlScriptDefinitionCache";
import TaskUtils from "../../../../utils/TaskUtils";
import { KafkaEtlPanel } from "./panels/KafkaEtlPanel";
import { RabbitMqEtlPanel } from "./panels/RabbitMqEtlPanel";
import useInterval from "hooks/useInterval";
import { Button, Col, Row } from "reactstrap";
import { HrHeader } from "components/common/HrHeader";
import { EmptySet } from "components/common/EmptySet";
import { Icon } from "components/common/Icon";
import OngoingTasksFilter, { OngoingTaskFilterType, OngoingTasksFilterCriteria } from "./OngoingTasksFilter";
import { exhaustiveStringTuple } from "components/utils/common";
import { InputItem } from "components/models/common";
import assertUnreachable from "components/utils/assertUnreachable";
import OngoingTaskSelectActions from "./OngoingTaskSelectActions";
import OngoingTaskToggleStateConfirm from "./OngoingTaskToggleStateConfirm";
import { StickyHeader } from "components/common/StickyHeader";

interface OngoingTasksPageProps {
    database: database;
}

export function OngoingTasksPage(props: OngoingTasksPageProps) {
    const { database } = props;

    const { canReadWriteDatabase, isClusterAdminOrClusterNode, isAdminAccessOrAbove } = useAccessManager();
    const { tasksService } = useServices();
    const [tasks, dispatch] = useReducer(ongoingTasksReducer, database, ongoingTasksReducerInitializer);

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

    const addNewOngoingTask = useCallback(() => {
        const addOngoingTaskView = new createOngoingTask(database);
        app.showBootstrapDialog(addOngoingTaskView);
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

    return (
        <div>
            {progressEnabled && <OngoingTaskProgressProvider db={database} onEtlProgress={onEtlProgress} />}
            {operationConfirm && (
                <OngoingTaskToggleStateConfirm {...operationConfirm} toggle={cancelOperationConfirm} />
            )}
            <StickyHeader>
                <Row>
                    <Col sm="auto" className="align-self-center">
                        {canReadWriteDatabase(database) && (
                            <Button onClick={addNewOngoingTask} color="primary" className="rounded-pill">
                                <Icon icon="plus" />
                                Add a Database Task
                            </Button>
                        )}
                    </Col>
                    <Col>
                        {allTasksCount > 0 && (
                            <OngoingTasksFilter
                                filter={filter}
                                setFilter={setFilter}
                                filterByStatusOptions={getFilterByStatusOptions(tasks)}
                                tasksCount={allTasksCount}
                            />
                        )}
                    </Col>
                    <Col sm="auto" className="align-self-center">
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
                    </Col>
                </Row>
            </StickyHeader>
            <div className="flex-vertical">
                <div className="scroll flex-grow">
                    {allTasksCount === 0 ? (
                        <EmptySet>No tasks have been created for this Database Group.</EmptySet>
                    ) : (
                        isAdminAccessOrAbove(database) && (
                            <OngoingTaskSelectActions
                                allTasks={filteredDatabaseTaskNames}
                                selectedTasks={selectedTaskNames}
                                setSelectedTasks={setSelectedTaskNames}
                                onTaskOperation={(type) => onTaskOperation(type, getSelectedTaskShardedInfos())}
                                isTogglingState={isTogglingStateAny}
                                isDeleting={isDeletingAny}
                            />
                        )
                    )}

                    {externalReplications.length > 0 && (
                        <div key="external-replications">
                            <HrHeader className="external-replication" count={externalReplications.length}>
                                <Icon icon="external-replication" />
                                External Replication
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

                    {subscriptions.length > 0 && (
                        <div key="subscriptions">
                            <HrHeader className="subscription" count={subscriptions.length}>
                                <Icon icon="subscription" />
                                Subscription
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
        </div>
    );
}

function getFilterByStatusOptions(state: OngoingTasksState): InputItem<OngoingTaskFilterType>[] {
    const backupCount = state.tasks.filter((x) => x.shared.taskType === "Backup").length;
    const etlCount = state.tasks.filter((x) => x.shared.taskType.endsWith("Etl")).length;
    const subscriptionsCount = state.subscriptions.length;
    const replicationHubCount = state.replicationHubs.length;
    const replicationSinkCount = state.tasks.filter((x) => x.shared.taskType === "PullReplicationAsSink").length;
    const externalReplicationCount = state.tasks.filter((x) => x.shared.taskType === "Replication").length;

    return exhaustiveStringTuple<OngoingTaskFilterType>()(
        "Backup",
        "ETL",
        "Subscription",
        "PullReplicationAsHub",
        "PullReplicationAsSink",
        "Replication"
    ).map((filterType) => {
        switch (filterType) {
            case "Backup":
                return { label: filterType, value: filterType, count: backupCount };
            case "ETL":
                return { label: filterType, value: filterType, count: etlCount };
            case "Subscription":
                return { label: filterType, value: filterType, count: subscriptionsCount };
            case "PullReplicationAsHub":
                return { label: "Replication Hub", value: filterType, count: replicationHubCount };
            case "PullReplicationAsSink":
                return { label: "Replication Sink", value: filterType, count: replicationSinkCount };
            case "Replication":
                return { label: "External Replication", value: filterType, count: externalReplicationCount };
            default:
                assertUnreachable(filterType);
        }
    });
}

function filterOngoingTask(sharedInfo: OngoingTaskSharedInfo, filter: OngoingTasksFilterCriteria) {
    const isTaskNameMatching = sharedInfo.taskName.toLowerCase().includes(filter.searchText.toLowerCase());
    const isETLTypeMatching = filter.types.includes("ETL") && sharedInfo.taskType.endsWith("Etl");
    const isOtherTypeMatching = filter.types.includes(sharedInfo.taskType as OngoingTaskFilterType);

    return isTaskNameMatching && (filter.types.length === 0 || isETLTypeMatching || isOtherTypeMatching);
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

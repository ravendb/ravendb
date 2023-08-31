import database from "models/resources/database";
import React, { ReactNode, useCallback, useEffect, useReducer, useState } from "react";
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
import { Alert, Badge, Button, Col, Modal, ModalBody, Row, UncontrolledTooltip } from "reactstrap";
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
import AboutViewFloating, { AccordionItemLicensing, AccordionItemWrapper } from "components/common/AboutView";
import classNames = require("classnames");
import { uniqueId } from "lodash";
import { FlexGrow } from "components/common/FlexGrow";

interface OngoingTasksPageProps {
    database: database;
}

export function OngoingTasksPage(props: OngoingTasksPageProps) {
    const { database } = props;

    const subscriptionsServerLimit = 3 * 5; //TODO
    const subscriptionsServerCount = 15; //TODO
    const subscriptionsDatabaseLimit = 3; //TODO

    const subscriptionsServerLimitStatus = getLicenseLimitReachStatus(
        subscriptionsServerLimit,
        subscriptionsServerCount
    );

    const [newTaskModal, setNewTaskModal] = useState(false);
    const toggleNewTaskModal = () => setNewTaskModal(!newTaskModal);

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

    return (
        <div>
            {subscriptionsServerLimitStatus !== "notReached" && (
                <Alert
                    color={subscriptionsServerLimitStatus === "limitReached" ? "danger" : "warning"}
                    className="text-center"
                >
                    Your server {subscriptionsServerLimitStatus === "limitReached" ? "reached" : "is reaching"} the{" "}
                    <strong>maximum number of subscriptions</strong> allowed by your license{" "}
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
            {progressEnabled && <OngoingTaskProgressProvider db={database} onEtlProgress={onEtlProgress} />}
            {operationConfirm && <OngoingTaskOperationConfirm {...operationConfirm} toggle={cancelOperationConfirm} />}
            <StickyHeader>
                <div className="hstack gap-3 flex-wrap">
                    {canReadWriteDatabase(database) && (
                        <Button onClick={addNewOngoingTask} color="primary" className="rounded-pill">
                            <Icon icon="ongoing-tasks" addon="plus" />
                            Add a Database Task
                        </Button>
                    )}
                    <Button color="primary" className="rounded-pill" onClick={toggleNewTaskModal}>
                        <Icon icon="ongoing-tasks" addon="plus" /> Add a Database Task (React)
                    </Button>

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
                            targetId="1"
                        >
                            <p>
                                <strong>Admin JS Console</strong> is a specialized feature primarily intended for
                                resolving server errors. It provides a direct interface to the underlying system,
                                granting the capacity to execute scripts for intricate server operations.
                            </p>
                            <p>
                                It is predominantly intended for advanced troubleshooting and rectification procedures
                                executed by system administrators or RavenDB support.
                            </p>
                            <hr />
                            <div className="small-label mb-2">useful links</div>
                            <a href="https://ravendb.net/l/IBUJ7M/6.0/Csharp" target="_blank">
                                <Icon icon="newtab" /> Docs - Admin JS Console
                            </a>
                        </AccordionItemWrapper>
                        <AccordionItemWrapper
                            icon="road-cone"
                            color="success"
                            heading="Examples of use"
                            description="Learn how to get the most of this feature"
                            targetId="2"
                        >
                            <p>
                                <strong>To set the refresh time:</strong> enter the appropriate date in the metadata{" "}
                                <code>@refresh</code> property.
                            </p>
                            <p>
                                <strong>Note:</strong> RavenDB scans which documents should be refreshed at the
                                frequency specified. The actual refresh time can increase (up to) that value.
                            </p>
                        </AccordionItemWrapper>
                        <AccordionItemWrapper
                            icon="license"
                            color="warning"
                            heading="Licensing"
                            description="See which plans offer this and more exciting features"
                            targetId="3"
                            pill
                            pillText="Upgrade available"
                            pillIcon="star-filled"
                        >
                            <AccordionItemLicensing
                                description="This feature is not available in your license. Unleash the full potential and upgrade your plan."
                                featureName="Document Compression"
                                featureIcon="documents-compression"
                                checkedLicenses={["Professional", "Enterprise"]}
                            >
                                <p className="lead fs-4">Get your license expanded</p>
                                <div className="mb-3">
                                    <Button color="primary" className="rounded-pill">
                                        <Icon icon="notifications" />
                                        Contact us
                                    </Button>
                                </div>
                                <small>
                                    <a href="#" target="_blank" className="text-muted">
                                        See pricing plans
                                    </a>
                                </small>
                            </AccordionItemLicensing>
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

                    {subscriptions.length > 0 && (
                        <div key="subscriptions">
                            <HrHeader className="subscription">
                                <Icon icon="subscription" />
                                Subscription
                                <CounterBadge
                                    count={subscriptions.length}
                                    limit={subscriptionsDatabaseLimit}
                                    className="ms-3"
                                />
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

            <Modal
                isOpen={newTaskModal}
                toggle={toggleNewTaskModal}
                container="modalContainer"
                contentClassName="modal-border bulge-primary"
                className="destination-modal"
                size="lg"
                centered
            >
                <ModalBody>
                    <div className="position-absolute m-2 end-0 top-0">
                        <Button close onClick={toggleNewTaskModal} />
                    </div>
                    <div className="vstack gap-4">
                        <div className="text-center">
                            <Icon icon="ongoing-tasks" color="primary" addon="plus" className="fs-1" margin="m-0" />
                        </div>
                        <div className="text-center lead">Add a Database Task</div>
                    </div>
                    <HrHeader>Replication</HrHeader>
                    <Row>
                        <TaskItem
                            title="Create new External Replication task"
                            className="external-replication"
                            disabled
                            disableReason="Feature available in Professional and Enterprise license"
                        >
                            <Icon icon="external-replication" />
                            <h4 className="mt-1 mb-0">External Replication</h4>
                            <Badge className="about-view-title-badge mt-2" color="faded-primary">
                                Professional +
                            </Badge>
                        </TaskItem>

                        <TaskItem
                            title="Create new Replication Hub task"
                            className="pull-replication-hub"
                            disabled
                            disableReason="Not supported in sharded databases"
                        >
                            <Icon icon="pull-replication-hub" />
                            <h4 className="mt-1 mb-0">Replication Hub</h4>
                        </TaskItem>
                        <TaskItem
                            title="Create new Replication Sink task"
                            className="pull-replication-sink"
                            disabled
                            disableReason="Not supported in sharded databases"
                        >
                            <Icon icon="pull-replication-agent" />
                            <h4 className="mt-1 mb-0">Replication Sink</h4>
                        </TaskItem>
                    </Row>
                    <HrHeader>ETL (RavenDB ⇛ TARGET)</HrHeader>
                    <Row>
                        <TaskItem
                            title="Create new RavenDB ETL task"
                            className="ravendb-etl"
                            disabled
                            disableReason="Feature available in Professional and Enterprise license"
                        >
                            <Icon icon="ravendb-etl" />
                            <h4 className="mt-1 mb-0">RavenDB ETL</h4>
                            <Badge className="about-view-title-badge mt-2" color="faded-primary">
                                Professional +
                            </Badge>
                        </TaskItem>

                        <TaskItem
                            title="Create new Elasticsearch ETL task"
                            className="elastic-etl"
                            disabled
                            disableReason="Feature available in Enterprise license"
                        >
                            <Icon icon="elastic-search-etl" />
                            <h4 className="mt-1 mb-0">Elasticsearch ETL</h4>
                            <Badge className="about-view-title-badge mt-2" color="faded-primary">
                                Enterprise
                            </Badge>
                        </TaskItem>

                        <TaskItem
                            title="Create new Kafka ETL task"
                            className="kafka-etl"
                            disabled
                            disableReason="Feature available in Enterprise license"
                        >
                            <Icon icon="kafka-etl" />
                            <h4 className="mt-1 mb-0">Kafka ETL</h4>
                            <Badge className="about-view-title-badge mt-2" color="faded-primary">
                                Enterprise
                            </Badge>
                        </TaskItem>

                        <TaskItem
                            title="Create new SQL ETL task"
                            className="sql-etl"
                            disabled
                            disableReason="Feature available in Professional and Enterprise license"
                        >
                            <Icon icon="sql-etl" />
                            <h4 className="mt-1 mb-0">SQL ETL</h4>
                            <Badge className="about-view-title-badge mt-2" color="faded-primary">
                                Professional +
                            </Badge>
                        </TaskItem>

                        <TaskItem
                            title="Create new OLAP ETL task"
                            className="olap-etl"
                            disabled
                            disableReason="Feature available in Enterprise license"
                        >
                            <Icon icon="olap-etl" />
                            <h4 className="mt-1 mb-0">OLAP ETL</h4>
                            <Badge className="about-view-title-badge mt-2" color="faded-primary">
                                Enterprise
                            </Badge>
                        </TaskItem>

                        <TaskItem
                            title="Create new RabbitMQ ETL task"
                            className="rabbitmq-etl"
                            disabled
                            disableReason="Feature available in Enterprise license"
                        >
                            <Icon icon="rabbitmq-etl" />
                            <h4 className="mt-1 mb-0">RabbitMQ ETL</h4>
                            <Badge className="about-view-title-badge mt-2" color="faded-primary">
                                Enterprise
                            </Badge>
                        </TaskItem>
                    </Row>
                    <HrHeader>SINK (SOURCE ⇛ RavenDB)</HrHeader>
                    <Row>
                        <TaskItem
                            title="Create new Kafka Sink task"
                            className="kafka-sink"
                            disabled
                            disableReason="Feature available in Professional and Enterprise license"
                        >
                            <Icon icon="kafka-sink" />
                            <h4 className="mt-1 mb-0">Kafka Sink</h4>
                            <Badge className="about-view-title-badge mt-2" color="faded-primary">
                                Professional +
                            </Badge>
                        </TaskItem>

                        <TaskItem
                            title="Create new RabbitMQ Sink task"
                            className="rabbitmq-sink"
                            disabled
                            disableReason="Not supported in sharded databases"
                        >
                            <Icon icon="rabbitmq-sink" />
                            <h4 className="mt-1 mb-0">RabbitMQ Sink</h4>
                        </TaskItem>
                    </Row>
                    <HrHeader>Backups & Subscriptions</HrHeader>
                    <Row>
                        <TaskItem
                            title="Create new Backup task"
                            className="backup"
                            disabled
                            disableReason="Feature available in Professional and Enterprise license"
                        >
                            <Icon icon="periodic-backup" />
                            <h4 className="mt-1 mb-0">Periodic Backup</h4>
                            <Badge className="about-view-title-badge mt-2" color="faded-primary">
                                Professional +
                            </Badge>
                        </TaskItem>

                        <TaskItem
                            title="Create new Subscription task"
                            className="subscription"
                            disabled
                            disableReason="License limit reached"
                        >
                            <Icon icon="subscription" />
                            <h4 className="mt-1 mb-0">Subscription</h4>
                            <CounterBadge className="mt-2" count={2} limit={3} hideNotReached />
                        </TaskItem>
                    </Row>
                </ModalBody>
            </Modal>
            <div id="modalContainer" className="bs5" />
        </div>
    );
}

interface TaskItemProps {
    title: string;
    className: string;
    children: ReactNode | ReactNode[];
    disabled?: boolean;
    disableReason?: string | ReactNode | ReactNode[];
}

export function TaskItem(props: TaskItemProps) {
    const { title, className, children, disabled, disableReason } = props;
    const TaskId = "Task" + uniqueId();

    return (
        <Col xs="6" md="4" className="mb-4 justify-content-center" title={title}>
            <a
                href="#"
                id={TaskId}
                className={classNames("task-item no-decor", className, { "item-disabled": disabled })}
            >
                {children}
            </a>
            {disableReason && <UncontrolledTooltip target={TaskId}>{disableReason}</UncontrolledTooltip>}
        </Col>
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

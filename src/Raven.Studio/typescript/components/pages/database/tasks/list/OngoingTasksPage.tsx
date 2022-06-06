import database from "models/resources/database";
import React, { useCallback, useEffect, useReducer } from "react";
import clusterTopologyManager from "common/shell/clusterTopologyManager";
import { useServices } from "hooks/useServices";
import { ongoingTasksReducer, ongoingTasksReducerInitializer } from "./OngoingTasksReducer";
import { useAccessManager } from "hooks/useAccessManager";
import createOngoingTask from "viewmodels/database/tasks/createOngoingTask";
import app from "durandal/app";
import useTimeout from "hooks/useTimeout";
import appUrl from "common/appUrl";
import { ExternalReplicationPanel } from "./panels/ExternalReplicationPanel";
import {
    OngoingTaskElasticSearchEtlInfo,
    OngoingTaskExternalReplicationInfo,
    OngoingTaskInfo,
    OngoingTaskOlapEtlInfo,
    OngoingTaskPeriodicBackupInfo,
    OngoingTaskRavenEtlInfo,
    OngoingTaskReplicationHubInfo,
    OngoingTaskReplicationSinkInfo,
    OngoingTaskSharedInfo,
    OngoingTaskSqlEtlInfo,
    OngoingTaskSubscriptionInfo,
} from "../../../../models/tasks";
import { RavenEtlPanel } from "./panels/RavenEtlPanel";
import { SqlEtlPanel } from "./panels/SqlEtlPanel";
import { OlapEtlPanel } from "./panels/OlapEtlPanel";
import { ElasticSearchEtlPanel } from "./panels/ElasticSearchEtlPanel";
import { PeriodicBackupPanel } from "./panels/PeriodicBackupPanel";
import { SubscriptionPanel } from "./panels/SubscriptionPanel";
import { ReplicationHubPanel } from "./panels/ReplicationHubPanel";
import { ReplicationSinkPanel } from "./panels/ReplicationSinkPanel";
import viewHelpers from "common/helpers/view/viewHelpers";
import genUtils from "common/generalUtils";
import ongoingTaskModel from "models/database/tasks/ongoingTaskModel";
import { ReplicationHubDefinitionPanel } from "./panels/ReplicationHubDefinitionPanel";

interface OngoingTasksPageProps {
    database: database;
}

export function OngoingTasksPage(props: OngoingTasksPageProps) {
    const { database } = props;

    const locations = database.getLocations();

    const { canReadWriteDatabase, isClusterAdminOrClusterNode } = useAccessManager();

    const { tasksService } = useServices();

    const [tasks, dispatch] = useReducer(ongoingTasksReducer, locations, ongoingTasksReducerInitializer);

    const fetchTasks = useCallback(
        async (location: databaseLocationSpecifier) => {
            const tasks = await tasksService.getOngoingTasks(database, location);
            dispatch({
                type: "TasksLoaded",
                location,
                tasks,
            });
        },
        [database]
    );

    const loadMissing = async () => {
        const loadTasks = tasks.tasks[0].nodesInfo.map(async (nodeInfo) => {
            if (nodeInfo.status === "notLoaded") {
                await fetchTasks(nodeInfo.location);
            }
        });

        await Promise.all(loadTasks);
    };

    useTimeout(loadMissing, 3_000);

    useEffect(() => {
        const nodeTag = clusterTopologyManager.default.localNodeTag();
        const initialLocation = database.getFirstLocation(nodeTag);

        fetchTasks(initialLocation);
    }, []);

    const addNewOngoingTask = useCallback(() => {
        const addOngoingTaskView = new createOngoingTask(database);
        app.showBootstrapDialog(addOngoingTaskView);
    }, [database]);

    const deleteTask = useCallback(
        async (task: OngoingTaskSharedInfo) => {
            await tasksService.deleteOngoingTask(database, task.taskType, task.taskId, task.taskName);

            //TODO: fetchOngoingTasks - reload!
        },
        [tasksService]
    );

    const onDeleteConfirmation = useCallback(
        (task: OngoingTaskSharedInfo) => {
            const taskType = ongoingTaskModel.mapTaskType(task.taskType);
            viewHelpers
                .confirmationMessage(
                    "Delete Ongoing Task?",
                    `You're deleting ${taskType} task: <br /><ul><li><strong>${genUtils.escapeHtml(
                        task.taskName
                    )}</strong></li></ul>`,
                    {
                        buttons: ["Cancel", "Delete"],
                        html: true,
                    }
                )
                .done((result) => {
                    if (result.can) {
                        deleteTask(task);
                    }
                });
        },
        [database]
    );

    const toggleOngoingTask = useCallback(
        async (task: OngoingTaskSharedInfo, enable: boolean) => {
            await tasksService.toggleOngoingTask(database, task.taskType, task.taskId, task.taskName, enable);
            //TODO: lazy update?
            //TODO: fetch ongoing tasks
        },
        [database, tasksService]
    );

    const onToggleStateConfirmation = useCallback(
        (task: OngoingTaskSharedInfo, enable: boolean) => {
            const confirmationTitle = enable ? "Enable Task" : "Disable Task";
            const taskType = ongoingTaskModel.mapTaskType(task.taskType);
            const confirmationMsg = enable
                ? `You're enabling ${taskType} task:<br><ul><li><strong>${task.taskName}</strong></li></ul>`
                : `You're disabling ${taskType} task:<br><ul><li><strong>${task.taskName}</strong></li></ul>`;
            const confirmButtonText = enable ? "Enable" : "Disable";

            viewHelpers
                .confirmationMessage(confirmationTitle, confirmationMsg, {
                    buttons: ["Cancel", confirmButtonText],
                    html: true,
                })
                .done((result) => {
                    if (result.can) {
                        toggleOngoingTask(task, enable);
                    }
                });
        },
        [toggleOngoingTask]
    );

    const canNavigateToServerWideTasks = isClusterAdminOrClusterNode();
    const serverWideTasksUrl = appUrl.forServerWideTasks();

    const externalReplications = tasks.tasks.filter(
        (x) => x.shared.taskType === "Replication"
    ) as OngoingTaskExternalReplicationInfo[];
    const ravenEtls = tasks.tasks.filter((x) => x.shared.taskType === "RavenEtl") as OngoingTaskRavenEtlInfo[];
    const sqlEtls = tasks.tasks.filter((x) => x.shared.taskType === "SqlEtl") as OngoingTaskSqlEtlInfo[];
    const olapEtls = tasks.tasks.filter((x) => x.shared.taskType === "OlapEtl") as OngoingTaskOlapEtlInfo[];
    const elasticSearchEtls = tasks.tasks.filter(
        (x) => x.shared.taskType === "ElasticSearchEtl"
    ) as OngoingTaskElasticSearchEtlInfo[];
    const backups = tasks.tasks.filter((x) => x.shared.taskType === "Backup") as OngoingTaskPeriodicBackupInfo[];
    const subscriptions = tasks.tasks.filter(
        (x) => x.shared.taskType === "Subscription"
    ) as OngoingTaskSubscriptionInfo[];
    const replicationHubs = tasks.tasks.filter(
        (x) => x.shared.taskType === "PullReplicationAsHub"
    ) as OngoingTaskReplicationHubInfo[];
    const replicationSinks = tasks.tasks.filter(
        (x) => x.shared.taskType === "PullReplicationAsSink"
    ) as OngoingTaskReplicationSinkInfo[];

    const hubDefinitions = tasks.replicationHubs;

    return (
        <div>
            <div className="flex-vertical">
                <div className="flex-header flex-horizontal">
                    {canReadWriteDatabase(database) && (
                        <button onClick={addNewOngoingTask} className="btn btn-primary">
                            <i className="icon-plus"></i>
                            <span>Add a Database Task</span>
                        </button>
                    )}
                    <div className="flex-separator"></div>
                    {canNavigateToServerWideTasks && (
                        <small className="padding padding-xs margin-left" title="Go to the Server-Wide Tasks view">
                            <a target="_blank" href={serverWideTasksUrl}>
                                <i className="icon-link"></i>Server-Wide Tasks
                            </a>
                        </small>
                    )}
                </div>
                <div className="scroll flex-grow">
                    {tasks.tasks.length === 0 && (
                        <div className="row">
                            <div className="col-sm-8 col-sm-offset-2 col-lg-6 col-lg-offset-3">
                                <i className="icon-xl icon-empty-set text-muted"></i>
                                <h2 className="text-center text-muted">
                                    No tasks have been created for this Database Group.
                                </h2>
                            </div>
                        </div>
                    )}

                    {externalReplications.length > 0 && (
                        <div key="external-replications">
                            <div className="hr-title">
                                <h5 className="tasks-list-item external-replication no-text-transform">
                                    <i className="icon-external-replication"></i>
                                    <span>External Replication ({externalReplications.length})</span>
                                </h5>
                                <hr />
                            </div>
                            <div>
                                {externalReplications.map((x) => (
                                    <ExternalReplicationPanel
                                        db={database}
                                        key={taskKey(x.shared)}
                                        data={x}
                                        onDelete={onDeleteConfirmation}
                                        toggleState={onToggleStateConfirmation}
                                    />
                                ))}
                            </div>
                        </div>
                    )}

                    {ravenEtls.length > 0 && (
                        <div key="raven-etls">
                            <div className="hr-title">
                                <h5 className="tasks-list-item ravendb-etl no-text-transform">
                                    <i className="icon-etl"></i>
                                    <span>RavenDB ETL ({ravenEtls.length})</span>
                                </h5>
                                <hr />
                            </div>
                            <div>
                                {ravenEtls.map((x) => (
                                    <RavenEtlPanel
                                        db={database}
                                        key={taskKey(x.shared)}
                                        data={x}
                                        onDelete={onDeleteConfirmation}
                                        toggleState={onToggleStateConfirmation}
                                    />
                                ))}
                            </div>
                        </div>
                    )}

                    {sqlEtls.length > 0 && (
                        <div key="sql-etls">
                            <div className="hr-title">
                                <h5 className="tasks-list-item sql-etl no-text-transform">
                                    <i className="icon-sql-etl"></i>
                                    <span>SQL ETL ({sqlEtls.length})</span>
                                </h5>
                                <hr />
                            </div>
                            <div>
                                {sqlEtls.map((x) => (
                                    <SqlEtlPanel
                                        db={database}
                                        key={taskKey(x.shared)}
                                        data={x}
                                        onDelete={onDeleteConfirmation}
                                        toggleState={onToggleStateConfirmation}
                                    />
                                ))}
                            </div>
                        </div>
                    )}

                    {olapEtls.length > 0 && (
                        <div key="olap-etls">
                            <div className="hr-title">
                                <h5 className="tasks-list-item olap-etl no-text-transform">
                                    <i className="icon-olap-etl"></i>
                                    <span>OLAP ETL ({olapEtls.length})</span>
                                </h5>
                                <hr />
                            </div>
                            <div>
                                {olapEtls.map((x) => (
                                    <OlapEtlPanel
                                        db={database}
                                        key={taskKey(x.shared)}
                                        data={x}
                                        onDelete={onDeleteConfirmation}
                                        toggleState={onToggleStateConfirmation}
                                    />
                                ))}
                            </div>
                        </div>
                    )}

                    {elasticSearchEtls.length > 0 && (
                        <div key="elastic-search-etls">
                            <div className="hr-title">
                                <h5 className="tasks-list-item elastic-etl no-text-transform">
                                    <i className="icon-elastic-search-etl"></i>
                                    <span>Elasticsearch ETL ({elasticSearchEtls.length})</span>
                                </h5>
                                <hr />
                            </div>
                            <div>
                                {elasticSearchEtls.map((x) => (
                                    <ElasticSearchEtlPanel
                                        db={database}
                                        key={taskKey(x.shared)}
                                        data={x}
                                        onDelete={onDeleteConfirmation}
                                        toggleState={onToggleStateConfirmation}
                                    />
                                ))}
                            </div>
                        </div>
                    )}

                    {backups.length > 0 && (
                        <div key="backups">
                            <div className="hr-title">
                                <h5 className="tasks-list-item periodic-backup no-text-transform">
                                    <i className="icon-backups"></i>
                                    <span>Periodic Backup ({backups.length})</span>
                                </h5>
                                <hr />
                            </div>
                            <div>
                                {backups.map((x) => (
                                    <PeriodicBackupPanel
                                        db={database}
                                        key={taskKey(x.shared)}
                                        data={x}
                                        onDelete={onDeleteConfirmation}
                                        toggleState={onToggleStateConfirmation}
                                    />
                                ))}
                            </div>
                        </div>
                    )}

                    {subscriptions.length > 0 && (
                        <div key="subscriptions">
                            <div className="hr-title">
                                <h5 className="tasks-list-item subscription no-text-transform">
                                    <i className="icon-subscription"></i>
                                    <span>Subscription ({subscriptions.length})</span>
                                </h5>
                                <hr />
                            </div>
                            <div>
                                {subscriptions.map((x) => (
                                    <SubscriptionPanel
                                        db={database}
                                        key={taskKey(x.shared)}
                                        data={x}
                                        onDelete={onDeleteConfirmation}
                                        toggleState={onToggleStateConfirmation}
                                    />
                                ))}
                            </div>
                        </div>
                    )}

                    {hubDefinitions.length > 0 && (
                        <div key="replication-hubs">
                            <div className="hr-title">
                                <h5 className="tasks-list-item pull-replication-hub no-text-transform">
                                    <i className="icon-pull-replication-hub"></i>
                                    <span>Replication Hub ({hubDefinitions.length})</span>
                                </h5>
                                <hr />
                            </div>
                            <div>
                                {hubDefinitions.map((def) => (
                                    <ReplicationHubDefinitionPanel
                                        db={database}
                                        key={taskKey(def.shared)}
                                        data={def}
                                        onDelete={onDeleteConfirmation}
                                        toggleState={onToggleStateConfirmation}
                                        connectedHubs={replicationHubs.filter(
                                            (x) => x.shared.taskName === def.shared.taskName
                                        )}
                                    />
                                ))}
                            </div>
                        </div>
                    )}

                    {replicationSinks.length > 0 && (
                        <div key="replication-sinks">
                            <div className="hr-title">
                                <h5 className="tasks-list-item pull-replication-sink no-text-transform">
                                    <i className="icon-pull-replication-agent"></i>
                                    <span>Replication Sink ({replicationSinks.length})</span>
                                </h5>
                                <hr />
                            </div>
                            <div>
                                {replicationSinks.map((x) => (
                                    <ReplicationSinkPanel
                                        db={database}
                                        key={taskKey(x.shared)}
                                        data={x}
                                        onDelete={onDeleteConfirmation}
                                        toggleState={onToggleStateConfirmation}
                                    />
                                ))}
                            </div>
                        </div>
                    )}

                    <h3>DEBUG:</h3>
                    <pre style={{ fontSize: "10px" }}>{JSON.stringify(tasks, null, 2)}</pre>
                </div>
            </div>
        </div>
    );
}

function taskKey(task: OngoingTaskSharedInfo) {
    return task.taskType + "-" + task.taskId;
}

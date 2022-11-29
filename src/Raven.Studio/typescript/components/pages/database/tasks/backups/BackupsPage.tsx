import database from "models/resources/database";
import React, { useCallback, useEffect, useReducer, useState } from "react";
import { PeriodicBackupPanel } from "../panels/PeriodicBackupPanel";
import { useAccessManager } from "hooks/useAccessManager";
import appUrl from "common/appUrl";
import { useServices } from "hooks/useServices";
import { ongoingTasksReducer, ongoingTasksReducerInitializer } from "../list/OngoingTasksReducer";
import useInterval from "hooks/useInterval";
import useTimeout from "hooks/useTimeout";
import clusterTopologyManager from "common/shell/clusterTopologyManager";
import { OngoingTaskInfo, OngoingTaskPeriodicBackupInfo, OngoingTaskSharedInfo } from "../../../../models/tasks";
import { BaseOngoingTaskPanelProps, taskKey } from "../shared";
import router from "plugins/router";
import PeriodicBackupStatus = Raven.Client.Documents.Operations.Backups.PeriodicBackupStatus;
import { loadableData } from "../../../../models/common";
import genUtils from "common/generalUtils";
import moment from "moment";
import { Button, Card, Row, Spinner } from "reactstrap";
import { HrHeader } from "../../../../../components/common/HrHeader";

interface manualBackupListModel {
    backupType: Raven.Client.Documents.Operations.Backups.BackupType;
    encrypted: boolean;
    nodeTag: string;
    destinations: string[];
    lastFullBackup: string;
}

function mapManualBackup(dto: PeriodicBackupStatus): manualBackupListModel {
    const destinations: string[] = [];

    if (dto.LocalBackup && dto.LocalBackup.BackupDirectory) {
        destinations.push("Local");
    }
    if (!dto.UploadToS3.Skipped) {
        destinations.push("S3");
    }
    if (!dto.UploadToGlacier.Skipped) {
        destinations.push("Glacier");
    }
    if (!dto.UploadToGlacier.Skipped) {
        destinations.push("Glacier");
    }
    if (!dto.UploadToAzure.Skipped) {
        destinations.push("Azure");
    }
    if (!dto.UploadToGoogleCloud.Skipped) {
        destinations.push("Google Cloud");
    }
    if (!dto.UploadToFtp.Skipped) {
        destinations.push("Ftp");
    }

    return {
        backupType: dto.BackupType,
        encrypted: dto.IsEncrypted,
        nodeTag: dto.NodeTag,
        destinations,
        lastFullBackup: dto.LastFullBackup,
    };
}

interface ManualBackupProps {
    model: loadableData<manualBackupListModel>;
}

function ManualBackup(props: ManualBackupProps) {
    const { model } = props;

    if (model.status === "error") {
        return <div className="bg-danger">Unable to load data: {model.error.responseJSON.Message}</div>;
    }

    if (model.status === "loading" || model.status === "notLoaded") {
        return (
            <div className="manual-backup">
                <Spinner />
            </div>
        );
    }

    const backup = model.data;

    if (!backup) {
        return (
            <div className="manual-backup">
                <div className="text-center">
                    <i className="icon-xl icon-empty-set text-muted"></i>
                    <div className="lead">No manual backup created</div>
                </div>
            </div>
        );
    }

    const lastFullBackupHumanized = backup.lastFullBackup
        ? genUtils.formatDurationByDate(moment.utc(backup.lastFullBackup), true)
        : "Never backed up";

    return (
        <div className="manual-backup">
            <div className="mt-1">
                <Card className="destination-item recent-backup">
                    <div className="padding flex-horizontal">
                        <div className="inline-properties">
                            <div className="property-item">
                                <div className="property-name">Recent Backup</div>
                                <div className="property-value" title={lastFullBackupHumanized}>
                                    {lastFullBackupHumanized}
                                </div>
                            </div>
                            <div className="property-item">
                                <div className="property-name">Type</div>
                                <div className="property-value">{backup.backupType}</div>
                            </div>
                            <div className="property-item">
                                <div className="property-name">Destinations</div>
                                <div className="property-value">
                                    {backup.destinations ? backup.destinations.join(", ") : "No destinations defined"}
                                </div>
                            </div>
                        </div>
                        <div className="flex-grow"></div>
                        <div>
                            {backup.encrypted ? (
                                <small title="Backup is encrypted">
                                    <i className="icon-encryption text-success"></i>
                                </small>
                            ) : (
                                <small title="Backup is not encrypted">
                                    <i className="icon-unlock text-gray"></i>
                                </small>
                            )}
                        </div>
                        <div className="node margin-right">
                            <div title="Cluster node that created this backup">
                                <i className="icon-cluster-node"></i>
                                <span>{backup.nodeTag}</span>
                            </div>
                        </div>
                    </div>
                </Card>
            </div>
        </div>
    );
}

interface BackupsPageProps {
    database: database;
}

export function BackupsPage(props: BackupsPageProps) {
    const { database } = props;
    const locations = database.getLocations();
    const { canReadWriteDatabase, isClusterAdminOrClusterNode, isAdminAccessOrAbove } = useAccessManager();

    const { tasksService } = useServices();
    const [manualBackup, setManualBackup] = useState<loadableData<manualBackupListModel>>({
        status: "notLoaded",
        data: null,
    });

    const [tasks, dispatch] = useReducer(ongoingTasksReducer, locations, ongoingTasksReducerInitializer);

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

    const fetchManualBackup = useCallback(
        async (silent = false) => {
            if (!silent) {
                setManualBackup({
                    data: null,
                    status: "loading",
                });
            }

            try {
                const manualBackup = await tasksService.getManualBackup(database);

                setManualBackup({
                    data: manualBackup.Status ? mapManualBackup(manualBackup.Status) : null,
                    status: "loaded",
                });
            } catch (e) {
                setManualBackup({
                    data: null,
                    error: e,
                    status: "error",
                });
            }
        },
        [database, tasksService]
    );

    const reload = useCallback(async () => {
        const loadTasks = tasks.locations.map((location) => fetchTasks(location));
        const loadManualBackup = fetchManualBackup(true);
        await Promise.all(loadTasks.concat(loadManualBackup));
    }, [tasks, fetchTasks, fetchManualBackup]);

    useInterval(reload, 10_000);

    const loadMissing = async () => {
        if (tasks.tasks.length > 0) {
            const loadTasks = tasks.tasks[0].nodesInfo.map(async (nodeInfo) => {
                if (nodeInfo.status === "notLoaded") {
                    await fetchTasks(nodeInfo.location);
                }
            });

            await Promise.all(loadTasks);
        }
    };

    useTimeout(loadMissing, 3_000);

    useEffect(() => {
        const nodeTag = clusterTopologyManager.default.localNodeTag();
        const initialLocation = database.getFirstLocation(nodeTag);

        // noinspection JSIgnoredPromiseFromCall
        fetchTasks(initialLocation);

        // noinspection JSIgnoredPromiseFromCall
        fetchManualBackup();
    }, [fetchManualBackup, fetchTasks, database]);

    const canNavigateToServerWideTasks = isClusterAdminOrClusterNode();
    const serverWideTasksUrl = appUrl.forServerWideTasks();

    const navigateToRestoreDatabase = () => {
        const url = appUrl.forDatabases("restore");
        router.navigate(url);
    };

    const deleteTask = useCallback(
        async (task: OngoingTaskSharedInfo) => {
            await tasksService.deleteOngoingTask(database, task);
            await reload();
        },
        [tasksService, reload, database]
    );

    const toggleOngoingTask = useCallback(
        async (task: OngoingTaskSharedInfo, enable: boolean) => {
            await tasksService.toggleOngoingTask(database, task, enable);
            await reload();
        },
        [database, tasksService, reload]
    );

    const sharedPanelProps: Omit<BaseOngoingTaskPanelProps<OngoingTaskInfo>, "data"> = {
        db: database,
        onDelete: deleteTask,
        toggleState: toggleOngoingTask,
    };

    const createNewPeriodicBackupTask = () => {
        const url = appUrl.forEditPeriodicBackupTask(database);
        router.navigate(url);
    };

    const createManualBackup = () => {
        const url = appUrl.forEditManualBackup(database);
        router.navigate(url);
    };

    const backups = tasks.tasks.filter((x) => x.shared.taskType === "Backup") as OngoingTaskPeriodicBackupInfo[];

    return (
        <div className="flex-grow-1 flex-stretch-items">
            <div className="flex-vertical">
                {isAdminAccessOrAbove(database) && (
                    <div className="flex-shrink-0 mb-4">
                        <Button
                            onClick={navigateToRestoreDatabase}
                            className=""
                            title="Navigate to creating a new database from a backup"
                        >
                            <i className="icon-restore-backup" />
                            <span>Restore a database from a backup</span>
                        </Button>
                    </div>
                )}

                <div className="flex-shrink-0">
                    <HrHeader>
                        <i className="icon-backup"></i>
                        <span>Manual Backup</span>
                    </HrHeader>

                    {isAdminAccessOrAbove(database) && (
                        <div className="mt-3 flex-shrink-0">
                            <Button color="primary" title="Backup the database now" onClick={createManualBackup}>
                                <i className="icon-backup"></i>
                                <span>Create a Backup</span>
                            </Button>
                        </div>
                    )}
                </div>

                <ManualBackup model={manualBackup} />

                <div className="flex-shrink-0 mt-5">
                    <HrHeader
                        right={
                            canNavigateToServerWideTasks && (
                                <Button
                                    size="xs"
                                    target="_blank"
                                    outline
                                    color="link"
                                    title="Navigate to the Server-Wide Tasks View"
                                    href={serverWideTasksUrl}
                                >
                                    Go to Server-Wide Tasks View
                                </Button>
                            )
                        }
                    >
                        <i className="icon-manage-ongoing-tasks"></i>
                        <span>Periodic Backup ({backups.length})</span>
                    </HrHeader>
                    {canReadWriteDatabase(database) && (
                        <div className="mt-3">
                            <Button
                                color="primary"
                                onClick={createNewPeriodicBackupTask}
                                title="Create an ongoing periodic backup task"
                            >
                                <i className="icon-backups"></i>
                                <span>Create a Periodic Backup</span>
                            </Button>
                        </div>
                    )}
                </div>

                <div className="flex-vertical">
                    <div className="scroll flex-grow">
                        <div key="backups">
                            {backups.length > 0 && (
                                <div>
                                    {backups.map((x) => (
                                        <PeriodicBackupPanel
                                            forceReload={reload}
                                            {...sharedPanelProps}
                                            key={taskKey(x.shared)}
                                            data={x}
                                        />
                                    ))}
                                </div>
                            )}
                        </div>

                        {backups.length === 0 && (
                            <div className="text-center">
                                <i className="icon-xl icon-empty-set text-muted"></i>
                                <div className="lead">No periodic backup tasks created</div>
                            </div>
                        )}
                    </div>
                </div>
            </div>
        </div>
    );
}

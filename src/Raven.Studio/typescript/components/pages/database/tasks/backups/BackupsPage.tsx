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
import { OngoingTaskInfo, OngoingTaskPeriodicBackupInfo, OngoingTaskSharedInfo } from "components/models/tasks";
import { BaseOngoingTaskPanelProps, taskKey } from "../shared";
import router from "plugins/router";
import PeriodicBackupStatus = Raven.Client.Documents.Operations.Backups.PeriodicBackupStatus;
import { loadableData } from "components/models/common";
import genUtils from "common/generalUtils";
import moment from "moment";
import { Button, Col, Row, Spinner } from "reactstrap";
import { HrHeader } from "components/common/HrHeader";
import { RichPanel, RichPanelDetailItem, RichPanelDetails, RichPanelHeader } from "components/common/RichPanel";
import { FlexGrow } from "components/common/FlexGrow";
import { EmptySet } from "components/common/EmptySet";
import { Icon } from "components/common/Icon";
import AboutView from "components/common/AboutView";

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

    if (model.status === "failure") {
        return <div className="bg-danger">Unable to load data: {model.error.responseJSON.Message}</div>;
    }

    if (model.status === "loading" || model.status === "idle") {
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
                <EmptySet>No manual backup created</EmptySet>
            </div>
        );
    }

    const lastFullBackupHumanized = backup.lastFullBackup
        ? genUtils.formatDurationByDate(moment.utc(backup.lastFullBackup), true)
        : "Never backed up";

    return (
        <RichPanel className="destination-item recent-backup">
            <RichPanelHeader className="flex-horizontal p-2">
                <RichPanelDetails className="p-0">
                    <RichPanelDetailItem label="Recent Backup" title={lastFullBackupHumanized}>
                        {lastFullBackupHumanized}
                    </RichPanelDetailItem>
                    <RichPanelDetailItem label="Type">{backup.backupType}</RichPanelDetailItem>
                    <RichPanelDetailItem label="Destinations">
                        {backup.destinations ? backup.destinations.join(", ") : "No destinations defined"}
                    </RichPanelDetailItem>
                </RichPanelDetails>

                <FlexGrow />
                <div className="flex-horizontal align-items-center p-2">
                    {backup.encrypted ? (
                        <div title="Backup is encrypted">
                            <Icon icon="encryption" color="success" margin="m-0" />
                        </div>
                    ) : (
                        <div title="Backup is not encrypted">
                            <Icon icon="unlock" color="muted" margin="m-0" />
                        </div>
                    )}

                    <div className="text-node ms-3" title="Cluster node that created this backup">
                        <Icon icon="cluster-node" />
                        <span>{backup.nodeTag}</span>
                    </div>
                </div>
            </RichPanelHeader>
        </RichPanel>
    );
}

interface BackupsPageProps {
    database: database;
}

export function BackupsPage(props: BackupsPageProps) {
    const { database } = props;
    const { canReadWriteDatabase, isClusterAdminOrClusterNode, isAdminAccessOrAbove } = useAccessManager();

    const { tasksService } = useServices();
    const [manualBackup, setManualBackup] = useState<loadableData<manualBackupListModel>>({
        status: "idle",
        data: null,
    });

    const [tasks, dispatch] = useReducer(ongoingTasksReducer, database, ongoingTasksReducerInitializer);

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
                    status: "success",
                });
            } catch (e) {
                setManualBackup({
                    data: null,
                    error: e,
                    status: "failure",
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
                if (nodeInfo.status === "idle") {
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
                    <div className="flex-shrink-0 hstack gap-4 mb-4">
                        <Button
                            onClick={navigateToRestoreDatabase}
                            title="Navigate to creating a new database from a backup"
                        >
                            <Icon icon="restore-backup" /> Restore a database from a backup
                        </Button>
                        <FlexGrow />
                        <AboutView>
                            <Row>
                                <Col sm={"auto"}>
                                    <Icon className="fs-1" icon="backup" color="info" margin="m-0" />
                                </Col>
                                <Col>
                                    <p>
                                        <strong>Backups</strong> save your data at a specific point in time and allow
                                        you to restore your database from that point.
                                    </p>
                                    <p>
                                        This Studio view enables you to create ongoing periodic backup tasks, as well as
                                        one-time manual backups, for a particular database.
                                    </p>
                                    <hr />
                                    <div className="small-label mb-2">useful links</div>
                                    <a href="https://ravendb.net/l/GMBYOH/6.0/Csharp">
                                        <Icon icon="newtab" /> Docs - Backups
                                    </a>
                                </Col>
                            </Row>
                        </AboutView>
                    </div>
                )}

                <div className="flex-shrink-0">
                    <HrHeader>
                        <Icon icon="backup" />
                        <span>Manual Backup</span>
                    </HrHeader>

                    {isAdminAccessOrAbove(database) && (
                        <div className="mb-1 flex-shrink-0">
                            <Button color="primary" title="Backup the database now" onClick={createManualBackup}>
                                <Icon icon="backup" /> Create a Backup
                            </Button>
                        </div>
                    )}
                </div>

                <ManualBackup model={manualBackup} />

                <div className="flex-shrink-0">
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
                        <Icon icon="manage-ongoing-tasks" />
                        <span>Periodic Backup ({backups.length})</span>
                    </HrHeader>
                    {canReadWriteDatabase(database) && (
                        <div className="mb-1">
                            <Button
                                color="primary"
                                onClick={createNewPeriodicBackupTask}
                                title="Create an ongoing periodic backup task"
                            >
                                <Icon icon="backup" /> Create a Periodic Backup
                            </Button>
                        </div>
                    )}
                </div>

                <div className="flex-vertical">
                    <div className="scroll flex-grow">
                        <div key="backups">
                            {backups.length > 0 && (
                                <>
                                    {backups.map((x) => (
                                        <PeriodicBackupPanel
                                            forceReload={reload}
                                            {...sharedPanelProps}
                                            key={taskKey(x.shared)}
                                            data={x}
                                        />
                                    ))}
                                </>
                            )}
                        </div>

                        {backups.length === 0 && <EmptySet>No periodic backup tasks created</EmptySet>}
                    </div>
                </div>
            </div>
        </div>
    );
}

import React, { useMemo } from "react";
import {
    BaseOngoingTaskPanelProps,
    OngoingTaskActions,
    OngoingTaskName,
    OngoingTaskResponsibleNode,
    OngoingTaskStatus,
    useTasksOperations,
} from "../shared";
import { OngoingTaskPeriodicBackupInfo } from "../../../../../models/tasks";
import { useAccessManager } from "hooks/useAccessManager";
import { useAppUrls } from "hooks/useAppUrls";
import { RichPanel, RichPanelDetailItem, RichPanelDetails, RichPanelHeader } from "../../../../../common/RichPanel";
import genUtils from "common/generalUtils";
import moment = require("moment");
import assertUnreachable from "../../../../../utils/assertUnreachable";
import timeHelpers from "common/timeHelpers";
import BackupType = Raven.Client.Documents.Operations.Backups.BackupType;

type PeriodicBackupPanelProps = BaseOngoingTaskPanelProps<OngoingTaskPeriodicBackupInfo>;

const neverBackedUpText = "Never backed up";

function formatBackupType(backupType: BackupType, isFull: boolean) {
    if (!isFull) {
        return "Incremental";
    }
    switch (backupType) {
        case "Snapshot":
            return "Snapshot";
        case "Backup":
            return "Full Backup";
        default:
            assertUnreachable(backupType);
    }
}

function Details(props: PeriodicBackupPanelProps) {
    const { data } = props;

    const backupDestinationsHumanized = data.shared.backupDestinations.length
        ? data.shared.backupDestinations.join(", ")
        : "No destinations defined";

    const lastFullBackupHumanized = data.shared.lastIncrementalBackup
        ? genUtils.formatDurationByDate(moment.utc(data.shared.lastIncrementalBackup), true)
        : neverBackedUpText;

    const lastIncrementalBackupHumanized = data.shared.lastIncrementalBackup
        ? genUtils.formatDurationByDate(moment.utc(data.shared.lastIncrementalBackup), true)
        : null;

    const nextBackupHumanized = useMemo(() => {
        const nextBackup = data.shared.nextBackup;
        if (!nextBackup) {
            return "N/A";
        }
        /* TODO
        if (this.isRunningOnAnotherNode()) {
                this.textClass("text-info");
                // the backup is running on another node
                return `Backup is already running or should start shortly on node ${this.responsibleNode().NodeTag}`;
            }
         */

        const now = timeHelpers.utcNowWithSecondPrecision();
        const diff = moment.utc(nextBackup.DateTime).diff(now);

        const backupTypeText = formatBackupType(data.shared.backupType, nextBackup.IsFull);
        const formatDuration = genUtils.formatDuration(moment.duration(diff), true, 2, true);
        return `in ${formatDuration} (${backupTypeText})`;
    }, [data.shared]);

    const onGoingBackupHumanized = useMemo(() => {
        const onGoingBackup = data.shared.onGoingBackup;
        if (!onGoingBackup) {
            return null;
        }

        const fromDuration = genUtils.formatDurationByDate(moment.utc(onGoingBackup.StartTime), true);
        return `${fromDuration} (${formatBackupType(data.shared.backupType, onGoingBackup.IsFull)})`;
    }, [data.shared]);

    const retentionPolicyHumanized = useMemo(() => {
        const disabled = data.shared.retentionPolicy ? data.shared.retentionPolicy.Disabled : true;
        if (disabled) {
            return "No backups will be removed";
        }

        const retentionPolicyPeriod = data.shared.retentionPolicy
            ? data.shared.retentionPolicy.MinimumBackupAgeToKeep
            : "0.0:00:00";

        return genUtils.formatTimeSpan(retentionPolicyPeriod, true);
    }, [data.shared]);

    //TODO: lastExecutingNode - does it make sense in case of sharding?

    return (
        <RichPanelDetails>
            <RichPanelDetailItem>
                Destinations:
                <div className="value">{backupDestinationsHumanized}</div>
            </RichPanelDetailItem>
            <RichPanelDetailItem>
                Last executed on node:
                <div className="value">{data.shared.lastExecutingNodeTag || "N/A"}</div>
            </RichPanelDetailItem>
            <RichPanelDetailItem>
                Last {formatBackupType(data.shared.backupType, true)}
                <div className="value">{lastFullBackupHumanized}</div>
            </RichPanelDetailItem>
            {lastIncrementalBackupHumanized && (
                <RichPanelDetailItem>
                    Last Incremental Backup:
                    <div className="value">{lastIncrementalBackupHumanized}</div>
                </RichPanelDetailItem>
            )}
            <RichPanelDetailItem>
                Next Estimated Backup:
                <div className="value">{nextBackupHumanized}</div>
            </RichPanelDetailItem>
            {onGoingBackupHumanized && (
                <RichPanelDetailItem>
                    Backup Started:
                    <div className="value">{onGoingBackupHumanized}</div>
                </RichPanelDetailItem>
            )}
            <RichPanelDetailItem>
                Retention Policy:
                <div className="value">{retentionPolicyHumanized}</div>
            </RichPanelDetailItem>
        </RichPanelDetails>
    );
    /*
 TODO
    return (
                
                <div className="flex-noshrink flex-grow flex-start text-right">
                    <button
                        className="btn backup-now"
                        data-bind="click: backupNow, enable: isBackupNowEnabled(), visible: isBackupNowVisible(),
                                             css: { 'btn-default': !neverBackedUp(), 'btn-info': backupNowInProgress, 'btn-spinner': backupNowInProgress, 'btn-warning': !backupNowInProgress() && neverBackedUp() },
                                             attr: { 'title':  disabledBackupNowReason() || 'Click to trigger the backup task now' },
                                             requiredAccess: 'DatabaseAdmin'"
                    >
                        <i className="icon-backups"></i>
                        <span data-bind="text: backupNowInProgress() ? 'Show backup progress' : 'Backup now'"></span>
                    </button>
                    <button className="btn btn-default" data-bind="click: refreshBackupInfo" title="Refresh info">
                        <i className="icon-refresh"></i>
                    </button>
                </div>
            </div>
        </div>
    );*/
}

function BackupEncryption(props: { encrypted: boolean }) {
    return (
        <div>
            {props.encrypted ? (
                <small title="Backup is encrypted">
                    <i className="icon-encryption text-success"></i>
                </small>
            ) : (
                <small title="Backup is not encrypted">
                    <i className="icon-unlock text-gray"></i>
                </small>
            )}
        </div>
    );
}

export function PeriodicBackupPanel(props: PeriodicBackupPanelProps) {
    //TODO: hide state for server wide!
    const { db, data } = props;

    const { isAdminAccessOrAbove } = useAccessManager();
    const { forCurrentDatabase } = useAppUrls();

    const canEdit = isAdminAccessOrAbove(db) && !data.shared.serverWide;
    const editUrl = forCurrentDatabase.editPeriodicBackupTask(data.shared.taskId)();

    const { detailsVisible, toggleDetails, toggleStateHandler, onEdit, onDeleteHandler } = useTasksOperations(
        editUrl,
        props
    );

    return (
        <RichPanel>
            <RichPanelHeader>
                <OngoingTaskName task={data} canEdit={canEdit} editUrl={editUrl} />
                <OngoingTaskResponsibleNode task={data} />
                <BackupEncryption encrypted={data.shared.encrypted} />
                <OngoingTaskStatus task={data} canEdit={canEdit} toggleState={toggleStateHandler} />

                <OngoingTaskActions
                    task={data}
                    canEdit={canEdit}
                    onEdit={onEdit}
                    onDelete={onDeleteHandler}
                    toggleDetails={toggleDetails}
                />
            </RichPanelHeader>
            {detailsVisible && <Details {...props} />}
        </RichPanel>
    );
}

/* TODO

class ongoingTaskBackupListModel extends ongoingTaskListModel {
    
    private watchProvider: (task: ongoingTaskBackupListModel) => void;

    backupNowInProgress = ko.observable<boolean>(false);
    isRunningOnAnotherNode: KnockoutComputed<boolean>;
    disabledBackupNowReason = ko.observable<string>();
    isBackupNowEnabled: KnockoutComputed<boolean>;
    isBackupNowVisible: KnockoutComputed<boolean>;
    neverBackedUp = ko.observable<boolean>(false);
    fullBackupTypeName: KnockoutComputed<string>;
    isBackupEncrypted = ko.observable<boolean>();
    lastExecutingNode = ko.observable<string>();

    throttledRefreshBackupInfo: () => void;

    constructor(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskBackup, watchProvider: (task: ongoingTaskBackupListModel) => void) {
        super();
        
        this.throttledRefreshBackupInfo = _.throttle(() => this.refreshBackupInfo(false), 60 * 1000);
        
        this.watchProvider = watchProvider;
        this.update(dto);
        
        this.initializeObservables();
    }

    initializeObservables() {
        super.initializeObservables();
        
        this.editUrl = ko.pureComputed(()=> {
             const urls = appUrl.forCurrentDatabase();

             return urls.editPeriodicBackupTask(this.taskId)();
        });

        this.isBackupNowEnabled = ko.pureComputed(() => {
            if (this.nextBackupHumanized() === "N/A") {
                this.disabledBackupNowReason("No backup destinations");
                return false;
            }

            if (this.isRunningOnAnotherNode()) {
                // the backup is running on another node
                this.disabledBackupNowReason(`Backup in progress on node ${this.responsibleNode().NodeTag}`);
                return false;
            }

            this.disabledBackupNowReason(null);
            return true;
        });

        this.isBackupNowVisible = ko.pureComputed(() => {
            return  !this.isServerWide() || accessManager.default.isClusterAdminOrClusterNode();
        });
        
        this.isRunningOnAnotherNode = ko.pureComputed(() => {
            const responsibleNode = this.responsibleNode();
            if (!responsibleNode || !responsibleNode.NodeTag) {
                return false;
            }

            if (responsibleNode.NodeTag === clusterTopologyManager.default.localNodeTag()) {
                return false;
            }

            const nextBackup = this.nextBackup();
            if (!nextBackup) {
                return false;
            }

            const now = timeHelpers.utcNowWithSecondPrecision();
            const diff = moment.utc(nextBackup.DateTime).diff(now);
            return diff <= 0;
        });

        this.fullBackupTypeName = ko.pureComputed(() => this.getBackupType(this.backupType(), true));
    }

    update(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskBackup) {
        super.update(dto);

        if (this.onGoingBackup()) {
            this.watchProvider(this);
        }

        this.backupNowInProgress(!!this.onGoingBackup());
        
        this.isServerWide(this.taskName().startsWith(ongoingTaskBackupListModel.serverWideNamePrefixFromServer));
    }


    refreshBackupInfo(reportFailure: boolean) {
        if (connectionStatus.showConnectionLost()) {
            // looks like we don't have connection to server, skip index progress update 
            return $.Deferred<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskBackup>().fail();
        }

        return ongoingTaskInfoCommand.forBackup(this.activeDatabase(), this.taskId, reportFailure)
            .execute()
            .done((result: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskBackup) => this.update(result));
    }

    backupNow() {
        const db = this.activeDatabase();
        const onGoingBackup = this.onGoingBackup();
        if (onGoingBackup) {
            const runningOperationId = onGoingBackup.RunningBackupTaskId;
            if (runningOperationId) {
                notificationCenter.instance.openDetailsForOperationById(db, runningOperationId);
                return;
            }
        }

        const backupNowViewModel = new backupNow(this.getBackupType(this.backupType(), true));
        backupNowViewModel
            .result
            .done((confirmResult: backupNowConfirmResult) => {
                if (confirmResult.can) {
                    this.backupNowInProgress(true);

                    const task = new backupNowPeriodicCommand(this.activeDatabase(), this.taskId, confirmResult.isFullBackup, this.taskName());
                    task.execute()
                        .done((backupNowResult: Raven.Client.Documents.Operations.Backups.StartBackupOperationResult) => {
                            this.refreshBackupInfo(true);
                            this.watchProvider(this);

                            if (backupNowResult && clusterTopologyManager.default.localNodeTag() === backupNowResult.ResponsibleNode) {
                                // running on this node
                                const operationId = backupNowResult.OperationId;
                                if (!this.onGoingBackup()) {
                                    this.onGoingBackup({
                                        IsFull: confirmResult.isFullBackup,
                                        RunningBackupTaskId: operationId
                                    });
                                }
                                notificationCenter.instance.openDetailsForOperationById(db, operationId);
                            }
                        })
                        .fail(() => {
                            // we failed to start the backup task
                            this.backupNowInProgress(false);
                        });
                        // backupNowInProgress is set to false after operation is finished
                }
            });

        app.showBootstrapDialog(backupNowViewModel);
    }
}



    private watchBackupCompletion(task: ongoingTaskBackupListModel) {
        if (!this.watchedBackups.has(task.taskId)) {
            let intervalId = setInterval(() => {
                task.refreshBackupInfo(false)
                    .done(result => {
                        if (!result.OnGoingBackup) {
                            clearInterval(intervalId);
                            intervalId = null;
                            this.watchedBackups.delete(task.taskId);
                        }
                    })
            }, 3000);
            this.watchedBackups.set(task.taskId, intervalId as unknown as number);

            this.registerDisposable({
                dispose: () => {
                    if (intervalId) {
                        clearInterval(intervalId);
                        intervalId = null;
                        this.watchedBackups.delete(task.taskId);
                    }
                }
            });
        }
    }

 */

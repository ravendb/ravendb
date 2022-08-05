import React, { useMemo } from "react";
import {
    BaseOngoingTaskPanelProps,
    OngoingTaskActions,
    OngoingTaskName,
    OngoingTaskResponsibleNode,
    OngoingTaskStatus,
    useTasksOperations,
} from "../shared";
import { OngoingTaskPeriodicBackupInfo } from "../../../../models/tasks";
import { useAccessManager } from "hooks/useAccessManager";
import { useAppUrls } from "hooks/useAppUrls";
import { RichPanel, RichPanelDetailItem, RichPanelDetails, RichPanelHeader } from "../../../../common/RichPanel";
import genUtils from "common/generalUtils";
import moment = require("moment");
import assertUnreachable from "../../../../utils/assertUnreachable";
import timeHelpers from "common/timeHelpers";
import BackupType = Raven.Client.Documents.Operations.Backups.BackupType;
import classNames from "classnames";
import notificationCenter from "common/notifications/notificationCenter";
import backupNow = require("viewmodels/database/tasks/backupNow");
import app from "durandal/app";
import clusterTopologyManager from "common/shell/clusterTopologyManager";
import backupNowPeriodicCommand from "commands/database/tasks/backupNowPeriodicCommand";

type PeriodicBackupPanelProps = BaseOngoingTaskPanelProps<OngoingTaskPeriodicBackupInfo> & { forceReload: () => void };

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

function findBackupNowBlockReason(data: OngoingTaskPeriodicBackupInfo, runningOnAnotherNode: boolean): string {
    if (!data.shared.nextBackup) {
        return "No backup destinations";
    }

    if (runningOnAnotherNode) {
        return "Backup in progress on node " + data.shared.responsibleNodeTag;
    }

    return null;
}

function Details(props: PeriodicBackupPanelProps & { canEdit: boolean }) {
    const { data, canEdit, db, forceReload } = props;

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

        const now = timeHelpers.utcNowWithSecondPrecision();
        const diff = moment.utc(nextBackup.DateTime).diff(now);

        const backupTypeText = formatBackupType(data.shared.backupType, nextBackup.IsFull);
        const formatDuration = genUtils.formatDuration(moment.duration(diff), true, 2, true);
        return `in ${formatDuration} (${backupTypeText})`;
    }, [data.shared]);

    const onGoingBackup = data.nodesInfo.map((x) => x.details?.onGoingBackup).find((x) => x);
    const runningOnAnotherNode =
        onGoingBackup && data.shared.responsibleNodeTag !== clusterTopologyManager.default.localNodeTag();

    const onGoingBackupHumanized = useMemo(() => {
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

    const backupNowVisible = !data.shared.serverWide || canEdit;
    const backupNowBlockReason = findBackupNowBlockReason(data, runningOnAnotherNode);

    const backupNowInProgress = !!onGoingBackup;
    const neverBackedUp = !data.shared.lastFullBackup;

    const onBackupNow = () => {
        if (onGoingBackup && onGoingBackup.RunningBackupTaskId) {
            notificationCenter.instance.openDetailsForOperationById(db, onGoingBackup.RunningBackupTaskId);
            return;
        }

        const backupNowViewModel = new backupNow(formatBackupType(data.shared.backupType, true));

        backupNowViewModel.result.done((confirmResult: backupNowConfirmResult) => {
            if (confirmResult.can) {
                const task = new backupNowPeriodicCommand(
                    db,
                    data.shared.taskId,
                    confirmResult.isFullBackup,
                    data.shared.taskName
                );
                task.execute().done(
                    (backupNowResult: Raven.Client.Documents.Operations.Backups.StartBackupOperationResult) => {
                        forceReload();

                        if (
                            backupNowResult &&
                            clusterTopologyManager.default.localNodeTag() === backupNowResult.ResponsibleNode
                        ) {
                            // running on this node
                            const operationId = backupNowResult.OperationId;
                            notificationCenter.instance.openDetailsForOperationById(db, operationId);
                        }
                    }
                );
            }
        });

        app.showBootstrapDialog(backupNowViewModel);
    };

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

            <RichPanelDetailItem>
                {backupNowVisible && (
                    <button
                        type="button"
                        onClick={onBackupNow}
                        className={classNames("btn backup-now", {
                            "btn-default": !neverBackedUp,
                            "btn-info": backupNowInProgress,
                            "btn-spinner": backupNowInProgress,
                            "btn-warning": !backupNowInProgress && neverBackedUp,
                        })}
                        disabled={!!backupNowBlockReason}
                        title={backupNowBlockReason ?? "Click to trigger the backup task now"}
                    >
                        <i className="icon-backups"></i>
                        <span>{backupNowInProgress ? "Show backup progress" : "Backup now"}</span>
                    </button>
                )}
            </RichPanelDetailItem>
        </RichPanelDetails>
    );
}

function BackupEncryption(props: { encrypted: boolean }) {
    return (
        <div className="margin-left-xs margin-right-xs">
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
            {detailsVisible && <Details canEdit={canEdit} {...props} />}
        </RichPanel>
    );
}

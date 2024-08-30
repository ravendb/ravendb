import React, { useMemo } from "react";
import {
    BaseOngoingTaskPanelProps,
    OngoingTaskActions,
    OngoingTaskName,
    OngoingTaskResponsibleNode,
    OngoingTaskStatus,
    useTasksOperations,
} from "../../shared/shared";
import { OngoingTaskPeriodicBackupInfo } from "components/models/tasks";
import { useAppUrls } from "hooks/useAppUrls";
import {
    RichPanel,
    RichPanelActions,
    RichPanelDetailItem,
    RichPanelDetails,
    RichPanelHeader,
    RichPanelInfo,
    RichPanelSelect,
} from "components/common/RichPanel";
import genUtils from "common/generalUtils";
import moment = require("moment");
import assertUnreachable from "../../../../../utils/assertUnreachable";
import timeHelpers from "common/timeHelpers";
import BackupType = Raven.Client.Documents.Operations.Backups.BackupType;
import classNames from "classnames";
import notificationCenter from "common/notifications/notificationCenter";
import backupNow = require("viewmodels/database/tasks/backupNow");
import app from "durandal/app";
import backupNowPeriodicCommand from "commands/database/tasks/backupNowPeriodicCommand";
import { Badge, Collapse, Input, UncontrolledTooltip } from "reactstrap";
import { Icon } from "components/common/Icon";
import { useAppSelector } from "components/store";
import { clusterSelectors } from "components/common/shell/clusterSlice";
import useId from "hooks/useId";
import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";

interface PeriodicBackupPanelProps extends BaseOngoingTaskPanelProps<OngoingTaskPeriodicBackupInfo> {
    forceReload: () => void;
    allowSelect: boolean;
    sourceView: EditPeriodicBackupTaskSourceView;
}

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

function Details(props: PeriodicBackupPanelProps) {
    const { data, forceReload } = props;

    const backupDestinationsHumanized = data.shared.backupDestinations.length
        ? data.shared.backupDestinations.join(", ")
        : "No destinations defined";

    const lastFullBackupHumanized = data.shared.lastFullBackup
        ? genUtils.formatDurationByDate(moment.utc(data.shared.lastFullBackup), true)
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
        const originalDate = nextBackup.OriginalBackupTime;
        const originalDateText = originalDate
            ? ", delayed backup from: " + genUtils.formatUtcDateAsLocal(originalDate)
            : "";
        return `in ${formatDuration} (${backupTypeText}) ${originalDateText}`;
    }, [data.shared]);

    const localNodeTag = useAppSelector(clusterSelectors.localNodeTag);

    const isResponsible = data.shared.responsibleNodeTag === localNodeTag;

    const onGoingBackup = data.nodesInfo.map((x) => x.details?.onGoingBackup).find((x) => x);
    const runningOnAnotherNode = onGoingBackup && !isResponsible;

    const onGoingBackupHumanized = useMemo(() => {
        if (!onGoingBackup) {
            return null;
        }

        const fromDuration = genUtils.formatDurationByDate(moment.utc(onGoingBackup.StartTime), true);
        return `${fromDuration} (${formatBackupType(data.shared.backupType, onGoingBackup.IsFull)})`;
    }, [data.shared, onGoingBackup]);

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

    const backupNowBlockReason = findBackupNowBlockReason(data, runningOnAnotherNode);

    const backupNowInProgress = !!onGoingBackup;
    const neverBackedUp = !data.shared.lastFullBackup;

    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();

    const backupNowVisible = data.shared.serverWide || hasDatabaseAdminAccess;

    const onBackupNow = () => {
        if (onGoingBackup && onGoingBackup.RunningBackupTaskId) {
            notificationCenter.instance.openDetailsForOperationById(
                activeDatabaseTracker.default.database(),
                onGoingBackup.RunningBackupTaskId
            );
            return;
        }

        const backupNowViewModel = new backupNow(formatBackupType(data.shared.backupType, true));

        backupNowViewModel.result.done((confirmResult: backupNowConfirmResult) => {
            if (confirmResult.can) {
                const task = new backupNowPeriodicCommand(
                    databaseName,
                    data.shared.taskId,
                    confirmResult.isFullBackup,
                    data.shared.taskName
                );
                task.execute().done(
                    (backupNowResult: Raven.Client.Documents.Operations.Backups.StartBackupOperationResult) => {
                        forceReload();

                        const isCurrentNodeResponsibleResult = localNodeTag === backupNowResult.ResponsibleNode;

                        if (backupNowResult && isCurrentNodeResponsibleResult) {
                            // running on this node
                            const operationId = backupNowResult.OperationId;
                            notificationCenter.instance.openDetailsForOperationById(
                                activeDatabaseTracker.default.database(),
                                operationId
                            );
                        }
                    }
                );
            }
        });

        app.showBootstrapDialog(backupNowViewModel);
    };

    const backupTypeLabel = formatBackupType(data.shared.backupType, true);

    return (
        <RichPanelDetails>
            <RichPanelDetailItem label="Destinations">{backupDestinationsHumanized}</RichPanelDetailItem>
            <RichPanelDetailItem label="Last executed on node">
                {data.shared.lastExecutingNodeTag || "N/A"}
            </RichPanelDetailItem>
            <RichPanelDetailItem label={"Last " + backupTypeLabel}>{lastFullBackupHumanized}</RichPanelDetailItem>
            {lastIncrementalBackupHumanized && (
                <RichPanelDetailItem label="Last Incremental Backup">
                    {lastIncrementalBackupHumanized}
                </RichPanelDetailItem>
            )}
            <RichPanelDetailItem label="Next Estimated Backup">
                {nextBackupHumanized}
                {backupNowVisible && (
                    <Badge
                        type="button"
                        onClick={onBackupNow}
                        className={classNames("ms-1 rounded-pill backup-now", {
                            "bg-secondary": !neverBackedUp,
                            "bg-info": backupNowInProgress,
                            "bg-progress": backupNowInProgress,
                            "bg-warning": !backupNowInProgress && neverBackedUp,
                        })}
                        disabled={!!backupNowBlockReason}
                        title={backupNowBlockReason ?? "Click to trigger the backup task now"}
                    >
                        <Icon icon="backup" />
                        <span>{isResponsible && backupNowInProgress ? "Show backup progress" : "Backup now"}</span>
                    </Badge>
                )}
            </RichPanelDetailItem>
            {onGoingBackupHumanized && (
                <RichPanelDetailItem label="Backup Started">{onGoingBackupHumanized}</RichPanelDetailItem>
            )}
            <RichPanelDetailItem label="Retention Policy">{retentionPolicyHumanized}</RichPanelDetailItem>
        </RichPanelDetails>
    );
}

function BackupEncryption(props: { encrypted: boolean }) {
    return (
        <div>
            {props.encrypted ? (
                <Icon icon="encryption" color="success" title="Backup is encrypted" margin="m-0" />
            ) : (
                <Icon icon="unlock" color="muted" title="Backup is not encrypted" margin="m-0" />
            )}
        </div>
    );
}

export function PeriodicBackupPanel(props: PeriodicBackupPanelProps) {
    const { data, allowSelect, toggleSelection, isSelected, onTaskOperation, isDeleting, isTogglingState, sourceView } =
        props;

    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();
    const { forCurrentDatabase } = useAppUrls();

    const canEdit = hasDatabaseAdminAccess && !data.shared.serverWide;
    const isServerWide = data.shared.serverWide;
    const editUrl = forCurrentDatabase.editPeriodicBackupTask(sourceView, data.shared.taskId)();

    const { detailsVisible, toggleDetails, onEdit } = useTasksOperations(editUrl, props);

    const statusDropdownId = useId("statusDropdown");

    return (
        <RichPanel>
            <RichPanelHeader>
                <RichPanelInfo>
                    {allowSelect && canEdit && (
                        <RichPanelSelect>
                            <Input
                                type="checkbox"
                                onChange={(e) => toggleSelection(e.currentTarget.checked, data.shared)}
                                checked={isSelected(data.shared.taskId)}
                            />
                        </RichPanelSelect>
                    )}
                    <OngoingTaskName task={data} canEdit={canEdit} editUrl={editUrl} />
                </RichPanelInfo>
                <RichPanelActions>
                    <OngoingTaskResponsibleNode task={data} />
                    <BackupEncryption encrypted={data.shared.encrypted} />
                    {isServerWide && (
                        <UncontrolledTooltip target={statusDropdownId}>
                            Status can be managed on the Server-Wide Tasks page
                        </UncontrolledTooltip>
                    )}
                    <OngoingTaskStatus
                        task={data}
                        canEdit={canEdit}
                        onTaskOperation={onTaskOperation}
                        isTogglingState={isTogglingState(data.shared.taskId)}
                        id={statusDropdownId}
                    />

                    <OngoingTaskActions
                        task={data}
                        canEdit={canEdit}
                        onEdit={onEdit}
                        onTaskOperation={onTaskOperation}
                        toggleDetails={toggleDetails}
                        isDeleting={isDeleting(data.shared.taskId)}
                    />
                </RichPanelActions>
            </RichPanelHeader>
            <Collapse isOpen={detailsVisible}>
                <Details {...props} />
            </Collapse>
        </RichPanel>
    );
}

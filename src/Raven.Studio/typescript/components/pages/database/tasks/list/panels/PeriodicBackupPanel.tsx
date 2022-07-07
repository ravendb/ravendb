import React from "react";
import {
    BaseOngoingTaskPanelProps,
    OngoingTaskActions,
    OngoingTaskName,
    OngoingTaskStatus,
    useTasksOperations,
} from "../shared";
import { OngoingTaskPeriodicBackupInfo } from "../../../../../models/tasks";
import { useAccessManager } from "hooks/useAccessManager";
import { useAppUrls } from "hooks/useAppUrls";
import { RichPanel, RichPanelDetailItem, RichPanelDetails, RichPanelHeader } from "../../../../../common/RichPanel";

type PeriodicBackupPanelProps = BaseOngoingTaskPanelProps<OngoingTaskPeriodicBackupInfo>;

function Details(props: PeriodicBackupPanelProps) {
    const { data } = props;

    const backupDestinationsHumanized = data.shared.backupDestinations.length
        ? data.shared.backupDestinations.join(", ")
        : "No destinations defined";

    //TODO: backupDestinationsHumanized class: textClass
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
        </RichPanelDetails>
    );
    /*

    return (
        <div className="collapse panel-addon" data-bind="collapse: showDetails">
            <div className="padding-sm flex-horizontal flex-wrap">
                <div>
                    <div className="list-properties">
                        <div className="property-item" data-bind="visible: lastFullBackupHumanized">
                            <div
                                className="property-name"
                                data-bind="text: 'Last ' + fullBackupTypeName() + (fullBackupTypeName() === 'Snapshot' ? ':' : ' Backup:')"
                            ></div>
                            <div
                                className="property-value text-details"
                                data-bind="text: lastFullBackupHumanized"
                            ></div>
                        </div>
                        <div className="property-item" data-bind="visible: lastIncrementalBackupHumanized">
                            <div className="property-name">Last Incremental Backup:</div>
                            <div
                                className="property-value text-details"
                                data-bind="text: lastIncrementalBackupHumanized"
                            ></div>
                        </div>
                        <div className="property-item" data-bind="visible: nextBackup() && !onGoingBackup()">
                            <div className="property-name">Next Estimated Backup:</div>
                            <div className="property-value">
                                <span data-bind="text: nextBackupHumanized, attr: { class: textClass() }"></span>
                            </div>
                        </div>
                        <div className="property-item" data-bind="visible: onGoingBackup">
                            <div className="property-name">Backup Started:</div>
                            <div className="property-value">
                                <span data-bind="text: onGoingBackupHumanized, attr: { class: textClass() }"></span>
                            </div>
                        </div>
                        <div className="property-item">
                            <div className="property-name">Retention Policy:</div>
                            <div className="property-value text-details">
                                <span data-bind="text: retentionPolicyHumanized"></span>
                            </div>
                        </div>
                    </div>
                </div>
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

    //TODO: hide state for server wide!
    /* TODO
    <div>
	<small title="Backup is encrypted" data-bind="visible: isBackupEncrypted">
		<i className="icon-encryption text-success"></i>
	</small>
	<small title="Backup is not encrypted" data-bind="visible: !isBackupEncrypted()">
		<i className="icon-unlock text-gray"></i>
	</small>
</div>
     */
}

import React from "react";
import Collapse from "react-bootstrap/Collapse";
import appUrl from "common/appUrl";
import genUtils from "common/generalUtils";
import useBoolean from "hooks/useBoolean";
import {
    RichPanel,
    RichPanelActions,
    RichPanelDetailItem,
    RichPanelDetails,
    RichPanelHeader,
    RichPanelInfo,
} from "components/common/RichPanel";
import {
    BaseServerWideTaskPanelProps,
    ServerWideTaskActions,
    ServerWideTaskExcludedDatabases,
    ServerWideTaskName,
    ServerWideTaskResponsibleNode,
    ServerWideTaskSelect,
    ServerWideTaskStatus,
} from "./ServerWideTaskPanelShared";
import { ServerWideBackupTaskInfo } from "../serverWideTaskModels";
import { formatBackupType } from "components/pages/database/tasks/ongoingTasks/panels/PeriodicBackupPanel";

type ServerWideBackupPanelProps = BaseServerWideTaskPanelProps<ServerWideBackupTaskInfo>;

function Details(props: { task: ServerWideBackupTaskInfo }) {
    const { task } = props;

    return (
        <RichPanelDetails>
            <ServerWideTaskExcludedDatabases task={task} />
            <RichPanelDetailItem label="Backup type">{formatBackupType(task.backupType, true)}</RichPanelDetailItem>
            <RichPanelDetailItem label="Destinations">
                {task.backupDestinations.length > 0 ? task.backupDestinations.join(", ") : "No destinations defined"}
            </RichPanelDetailItem>
            <RichPanelDetailItem label="Encrypted">{task.isEncrypted ? "Yes" : "No"}</RichPanelDetailItem>
            {task.retentionPolicy && !task.retentionPolicy.Disabled && (
                <RichPanelDetailItem label="Retention">
                    {genUtils.formatTimeSpan(task.retentionPolicy.MinimumBackupAgeToKeep, true)}
                </RichPanelDetailItem>
            )}
        </RichPanelDetails>
    );
}

export function ServerWideBackupPanel(props: ServerWideBackupPanelProps) {
    const { task, isSelected, toggleSelection, onTaskOperation, isDeleting, isTogglingState } = props;

    const { value: detailsVisible, toggle: toggleDetails } = useBoolean(false);

    const editUrl = appUrl.forEditServerWideBackup(task.taskName);

    return (
        <RichPanel>
            <RichPanelHeader>
                <RichPanelInfo>
                    <ServerWideTaskSelect task={task} isSelected={isSelected} toggleSelection={toggleSelection} />
                    <ServerWideTaskName task={task} editUrl={editUrl} />
                </RichPanelInfo>
                <RichPanelActions>
                    <ServerWideTaskResponsibleNode task={task} />
                    <ServerWideTaskStatus
                        task={task}
                        onTaskOperation={onTaskOperation}
                        isTogglingState={isTogglingState(task.taskId)}
                    />
                    <ServerWideTaskActions
                        task={task}
                        editUrl={editUrl}
                        onTaskOperation={onTaskOperation}
                        isDeleting={isDeleting(task.taskId)}
                        detailsVisible={detailsVisible}
                        toggleDetails={toggleDetails}
                    />
                </RichPanelActions>
            </RichPanelHeader>
            <Collapse in={detailsVisible}>
                <div>
                    <Details task={task} />
                </div>
            </Collapse>
        </RichPanel>
    );
}

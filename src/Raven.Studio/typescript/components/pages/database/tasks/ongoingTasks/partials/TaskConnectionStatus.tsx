import React from "react";
import { RichPanelDetailItem } from "components/common/RichPanel";
import { OngoingTaskInfo } from "components/models/tasks";
import { Icon } from "components/common/Icon";
import { getVisibleNodes } from "./ExternalReplicationTaskDistribution";
import { connectionStatusRanks, taskConnectionStatusLabels } from "./taskConnectionStatusLabels";
import { WarningDetails } from "./WarningDetails";
import OngoingTaskConnectionStatus = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskConnectionStatus;

export function getTaskConnectionStatus(task: OngoingTaskInfo): OngoingTaskConnectionStatus | null {
    const statuses = getVisibleNodes(task).map((x) => x.details.taskConnectionStatus);

    if (statuses.length === 0) {
        return null;
    }

    return statuses.reduce((worst, x) => (connectionStatusRanks[x] < connectionStatusRanks[worst] ? x : worst));
}

function isProblemConnectionStatus(task: OngoingTaskInfo, connectionStatus: OngoingTaskConnectionStatus) {
    return (
        connectionStatus === "Reconnect" || (connectionStatus === "NotActive" && task.shared.taskState === "Enabled")
    );
}

interface TaskConnectionStatusItemProps {
    task: OngoingTaskInfo;
    reconnectDetails?: string;
}

export function TaskConnectionStatusItem({ task, reconnectDetails }: TaskConnectionStatusItemProps) {
    const connectionStatus = getTaskConnectionStatus(task);

    if (!connectionStatus) {
        return null;
    }

    const label = taskConnectionStatusLabels[connectionStatus];

    if (connectionStatus === "Reconnect" && reconnectDetails) {
        return (
            <RichPanelDetailItem label="Connection Status">
                <WarningDetails details={reconnectDetails} title="Show reconnect details">
                    {label}
                </WarningDetails>
            </RichPanelDetailItem>
        );
    }

    return (
        <RichPanelDetailItem label="Connection Status">
            {isProblemConnectionStatus(task, connectionStatus) ? (
                <span className="text-warning">
                    <Icon icon="warning" margin="me-1" />
                    {label}
                </span>
            ) : (
                label
            )}
        </RichPanelDetailItem>
    );
}

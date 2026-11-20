import React from "react";
import { RichPanelDetailItem } from "components/common/RichPanel";
import { OngoingTaskInfo } from "components/models/tasks";
import { getVisibleNodes } from "./ExternalReplicationTaskDistribution";
import OngoingTaskConnectionStatus = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskConnectionStatus;

export const taskConnectionStatusLabels: Record<OngoingTaskConnectionStatus, string> = {
    None: "None",
    Active: "Active",
    NotActive: "Not active",
    Reconnect: "Reconnect",
    NotOnThisNode: "Not on this node",
};

export const connectionStatusRanks: Record<OngoingTaskConnectionStatus, number> = {
    Reconnect: 1,
    NotActive: 2,
    None: 3,
    NotOnThisNode: 4,
    Active: 5,
};

export function getTaskConnectionStatus(task: OngoingTaskInfo): OngoingTaskConnectionStatus | null {
    const statuses = getVisibleNodes(task).map((x) => x.details.taskConnectionStatus);

    if (statuses.length === 0) {
        return null;
    }

    return statuses.reduce((worst, x) => (connectionStatusRanks[x] < connectionStatusRanks[worst] ? x : worst));
}

export function TaskConnectionStatusItem({ task, label }: { task: OngoingTaskInfo; label: string }) {
    const connectionStatus = getTaskConnectionStatus(task);

    if (!connectionStatus) {
        return null;
    }

    return <RichPanelDetailItem label={label}>{taskConnectionStatusLabels[connectionStatus]}</RichPanelDetailItem>;
}

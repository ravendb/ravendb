import React from "react";
import {
    OngoingTaskExternalReplicationInfo,
    OngoingTaskReplicationHubInfo,
    OngoingTaskReplicationSinkInfo,
} from "components/models/tasks";
import { RichPanelDetailItem } from "components/common/RichPanel";
import { getVisibleNodes, ReplicationTaskSummaryProgress } from "./ExternalReplicationTaskDistribution";
import { ErrorDetailItem } from "./ErrorModal";
import { TaskConnectionStatusItem } from "./TaskConnectionStatus";

export type ReplicationTaskInfo =
    | OngoingTaskExternalReplicationInfo
    | OngoingTaskReplicationHubInfo
    | OngoingTaskReplicationSinkInfo;

export function getReplicationError(task: ReplicationTaskInfo): string | null {
    return getVisibleNodes(task).find((x) => x.details.error)?.details.error ?? null;
}

export function ReplicationSummaryItems({ task }: { task: ReplicationTaskInfo }) {
    const visibleNodes = getVisibleNodes(task);
    const error = getReplicationError(task);

    return (
        <>
            <TaskConnectionStatusItem task={task} label="Replication Status" />
            {error && <ErrorDetailItem error={error} />}
            {visibleNodes.length > 0 && (
                <RichPanelDetailItem>
                    <ReplicationTaskSummaryProgress task={task} nodesInfo={visibleNodes} />
                </RichPanelDetailItem>
            )}
        </>
    );
}

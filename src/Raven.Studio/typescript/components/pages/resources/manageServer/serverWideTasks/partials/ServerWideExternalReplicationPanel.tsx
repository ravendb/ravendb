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
import { ServerWideExternalReplicationTaskInfo } from "../serverWideTaskModels";

type ServerWideExternalReplicationPanelProps = BaseServerWideTaskPanelProps<ServerWideExternalReplicationTaskInfo>;

function Details(props: { task: ServerWideExternalReplicationTaskInfo }) {
    const { task } = props;

    return (
        <RichPanelDetails>
            <ServerWideTaskExcludedDatabases task={task} />
            <RichPanelDetailItem label="Topology discovery URLs">
                {task.topologyDiscoveryUrls.length > 0 ? task.topologyDiscoveryUrls.join(", ") : "No URLs defined"}
            </RichPanelDetailItem>
            {task.delayReplicationFor && task.delayReplicationFor !== "00:00:00" && (
                <RichPanelDetailItem label="Replication delay">
                    {genUtils.formatTimeSpan(task.delayReplicationFor, true)}
                </RichPanelDetailItem>
            )}
        </RichPanelDetails>
    );
}

export function ServerWideExternalReplicationPanel(props: ServerWideExternalReplicationPanelProps) {
    const { task, isSelected, toggleSelection, onTaskOperation, isDeleting, isTogglingState } = props;

    const { value: detailsVisible, toggle: toggleDetails } = useBoolean(false);

    const editUrl = appUrl.forEditServerWideExternalReplication(task.taskName);

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

import React from "react";
import {
    BaseOngoingTaskPanelProps,
    OngoingTaskActions,
    OngoingTaskName,
    OngoingTaskStatus,
    useTasksOperations,
} from "../shared";
import {
    OngoingTaskExternalReplicationInfo,
    OngoingTaskHubDefinitionInfo,
    OngoingTaskHubDefinitionSharedInfo,
    OngoingTaskReplicationHubInfo,
    OngoingTaskReplicationHubSharedInfo,
    OngoingTaskSharedInfo,
} from "../../../../../models/tasks";
import database from "models/resources/database";
import { RichPanel, RichPanelHeader } from "../../../../../common/RichPanel";
import { useAppUrls } from "hooks/useAppUrls";
import { useAccessManager } from "hooks/useAccessManager";
import { ReplicationHubPanel } from "./ReplicationHubPanel";

interface ReplicationHubPanelProps {
    db: database;
    data: OngoingTaskHubDefinitionInfo;
    onDelete: (task: OngoingTaskSharedInfo) => void;
    toggleState: (task: OngoingTaskSharedInfo, enable: boolean) => void;
    connectedHubs: OngoingTaskReplicationHubInfo[];
}

function Details(props: ReplicationHubPanelProps & { canEdit: boolean }) {
    const { connectedHubs, db } = props;

    /* TODO
    return (
        <div className="property-item" data-bind="visible: showDelayReplication">
            <div className="property-name">Replication Delay Time:</div>
            <div className="property-value" data-bind="text: delayHumane" title="Replication Delay Time"></div>
        </div>
    );
     */

    if (connectedHubs.length === 0) {
        return (
            <h5 className="text-warning padding-sm">
                <i className="icon-empty-set"></i>
                <span>No sinks connected</span>
            </h5>
        );
    }

    return (
        <div className="margin">
            {connectedHubs.map((hub) => (
                <ReplicationHubPanel key={hub.shared.taskId + hub.shared.taskName} db={db} data={hub} />
            ))}
        </div>
    );
}

export function ReplicationHubDefinitionPanel(props: ReplicationHubPanelProps) {
    const { data, db } = props;

    const { isAdminAccessOrAbove } = useAccessManager();

    const { forCurrentDatabase } = useAppUrls();
    const editUrl = forCurrentDatabase.editReplicationHub(data.shared.taskId)();
    const canEdit = isAdminAccessOrAbove(db) && !data.shared.serverWide;
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
            {detailsVisible && <Details {...props} canEdit={canEdit} />}
        </RichPanel>
    );
}

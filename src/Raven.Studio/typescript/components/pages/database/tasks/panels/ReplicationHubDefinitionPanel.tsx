import React from "react";
import { OngoingTaskActions, OngoingTaskName, OngoingTaskStatus, useTasksOperations } from "../shared";
import {
    OngoingTaskHubDefinitionInfo,
    OngoingTaskReplicationHubInfo,
    OngoingTaskSharedInfo,
} from "../../../../models/tasks";
import database from "models/resources/database";
import { RichPanel, RichPanelDetailItem, RichPanelDetails, RichPanelHeader } from "../../../../common/RichPanel";
import { useAppUrls } from "hooks/useAppUrls";
import { useAccessManager } from "hooks/useAccessManager";
import { ReplicationHubConnectedSinkPanel } from "./ReplicationHubConnectedSinkPanel";
import genUtils from "common/generalUtils";

interface ReplicationHubPanelProps {
    db: database;
    data: OngoingTaskHubDefinitionInfo;
    onDelete: (task: OngoingTaskSharedInfo) => void;
    toggleState: (task: OngoingTaskSharedInfo, enable: boolean) => void;
    connectedSinks: OngoingTaskReplicationHubInfo[];
}

function Details(props: ReplicationHubPanelProps & { canEdit: boolean }) {
    const { connectedSinks, db, data } = props;

    const delayHumane = data.shared.delayReplicationTime
        ? genUtils.formatTimeSpan(data.shared.delayReplicationTime * 1000, true)
        : null;

    return (
        <div>
            <RichPanelDetails>
                {delayHumane && (
                    <RichPanelDetailItem>
                        Replication Delay Time:
                        <div className="value">{delayHumane}</div>
                    </RichPanelDetailItem>
                )}
                <RichPanelDetailItem>
                    Replication Mode:
                    <div className="value">{data.shared.taskMode}</div>
                </RichPanelDetailItem>
                <RichPanelDetailItem>
                    Has Filtering:
                    <div className="value">{data.shared.hasFiltering ? "True" : "False"}</div>
                </RichPanelDetailItem>
            </RichPanelDetails>
            {connectedSinks.length > 0 && (
                <div className="margin">
                    {connectedSinks.map((sink) => (
                        <ReplicationHubConnectedSinkPanel
                            key={sink.shared.taskId + sink.shared.taskName}
                            db={db}
                            data={sink}
                        />
                    ))}
                </div>
            )}
            <div className="margin-left">
                {connectedSinks.length === 0 && (
                    <h5 className="text-warning padding-sm">
                        <i className="icon-empty-set"></i>
                        <span>No sinks connected</span>
                    </h5>
                )}
            </div>
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

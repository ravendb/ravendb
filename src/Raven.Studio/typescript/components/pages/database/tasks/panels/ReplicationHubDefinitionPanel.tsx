import React from "react";
import { OngoingTaskActions, OngoingTaskName, OngoingTaskStatus, useTasksOperations } from "../shared";
import {
    OngoingTaskHubDefinitionInfo,
    OngoingTaskReplicationHubInfo,
    OngoingTaskSharedInfo,
} from "../../../../models/tasks";
import database from "models/resources/database";
import {
    RichPanel,
    RichPanelActions,
    RichPanelDetailItem,
    RichPanelDetails,
    RichPanelHeader,
    RichPanelInfo,
} from "../../../../common/RichPanel";
import { useAppUrls } from "hooks/useAppUrls";
import { useAccessManager } from "hooks/useAccessManager";
import { ReplicationHubConnectedSinkPanel } from "./ReplicationHubConnectedSinkPanel";
import genUtils from "common/generalUtils";
import { Collapse } from "reactstrap";

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
                {delayHumane && <RichPanelDetailItem label="Replication Delay Time">{delayHumane}</RichPanelDetailItem>}
                <RichPanelDetailItem label="Replication Mode">{data.shared.taskMode}</RichPanelDetailItem>
                <RichPanelDetailItem label="Has Filtering">
                    {data.shared.hasFiltering ? "True" : "False"}
                </RichPanelDetailItem>
            </RichPanelDetails>
            {connectedSinks.length > 0 && (
                <div className="my-1 mx-3">
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
                <RichPanelInfo>
                    <OngoingTaskName task={data} canEdit={canEdit} editUrl={editUrl} />
                </RichPanelInfo>
                <RichPanelActions>
                    <OngoingTaskStatus task={data} canEdit={canEdit} toggleState={toggleStateHandler} />
                    <OngoingTaskActions
                        task={data}
                        canEdit={canEdit}
                        onEdit={onEdit}
                        onDelete={onDeleteHandler}
                        toggleDetails={toggleDetails}
                    />
                </RichPanelActions>
            </RichPanelHeader>
            <Collapse isOpen={detailsVisible}>
                <Details {...props} canEdit={canEdit} />
            </Collapse>
        </RichPanel>
    );
}

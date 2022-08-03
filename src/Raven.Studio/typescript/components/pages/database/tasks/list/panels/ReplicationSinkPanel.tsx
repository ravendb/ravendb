import React from "react";
import {
    BaseOngoingTaskPanelProps,
    ConnectionStringItem,
    OngoingTaskActions,
    OngoingTaskName,
    OngoingTaskResponsibleNode,
    OngoingTaskStatus,
    useTasksOperations,
} from "../shared";
import { OngoingTaskReplicationSinkInfo } from "../../../../../models/tasks";
import { useAccessManager } from "hooks/useAccessManager";
import { useAppUrls } from "hooks/useAppUrls";
import { RichPanel, RichPanelDetailItem, RichPanelDetails, RichPanelHeader } from "../../../../../common/RichPanel";

type ReplicationSinkPanelProps = BaseOngoingTaskPanelProps<OngoingTaskReplicationSinkInfo>;

function Details(props: ReplicationSinkPanelProps & { canEdit: boolean }) {
    const { data, canEdit, db } = props;
    const connectionStringDefined = !!data.shared.destinationDatabase;
    const { appUrl } = useAppUrls();
    const connectionStringsUrl = appUrl.forConnectionStrings(db, "Raven", data.shared.connectionStringName);

    return (
        <RichPanelDetails>
            <RichPanelDetailItem>
                Hub Name:
                <div className="value">{data.shared.hubName}</div>
            </RichPanelDetailItem>
            <ConnectionStringItem
                connectionStringDefined={!!data.shared.destinationDatabase}
                canEdit={canEdit}
                connectionStringName={data.shared.connectionStringName}
                connectionStringsUrl={connectionStringsUrl}
            />
            {connectionStringDefined && (
                <RichPanelDetailItem>
                    Hub Database:
                    <div className="value">{data.shared.destinationDatabase}</div>
                </RichPanelDetailItem>
            )}
            <RichPanelDetailItem>
                Actual Hub URL:
                <div className="value">{data.shared.destinationUrl ?? "N/A"}</div>
            </RichPanelDetailItem>

            {data.shared.topologyDiscoveryUrls.map((url) => (
                <RichPanelDetailItem key={url}>
                    Topology Discovery URL:
                    <div className="value">{url}</div>
                </RichPanelDetailItem>
            ))}
        </RichPanelDetails>
    );
}

export function ReplicationSinkPanel(props: ReplicationSinkPanelProps) {
    const { db, data } = props;

    const { isAdminAccessOrAbove } = useAccessManager();
    const { forCurrentDatabase } = useAppUrls();

    const canEdit = isAdminAccessOrAbove(db) && !data.shared.serverWide;
    const editUrl = forCurrentDatabase.editReplicationSink(data.shared.taskId)();

    const { detailsVisible, toggleDetails, toggleStateHandler, onEdit, onDeleteHandler } = useTasksOperations(
        editUrl,
        props
    );

    return (
        <RichPanel>
            <RichPanelHeader>
                <OngoingTaskName task={data} canEdit={canEdit} editUrl={editUrl} />
                <OngoingTaskResponsibleNode task={data} />
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

import React from "react";
import { OngoingTaskExternalReplicationInfo } from "../../../../../models/tasks";
import { RichPanel, RichPanelDetailItem, RichPanelDetails, RichPanelHeader } from "../../../../../common/RichPanel";
import {
    ConnectionStringItem,
    OngoingTaskActions,
    OngoingTaskName,
    OngoingTaskResponsibleNode,
    OngoingTaskStatus,
} from "../shared";
import { useAccessManager } from "hooks/useAccessManager";
import { useAppUrls } from "hooks/useAppUrls";
import { BaseOngoingTaskPanelProps, useTasksOperations } from "../shared";
import genUtils from "common/generalUtils";

type ExternalReplicationPanelProps = BaseOngoingTaskPanelProps<OngoingTaskExternalReplicationInfo>;

function Details(props: ExternalReplicationPanelProps & { canEdit: boolean }) {
    const { data, canEdit, db } = props;

    const showDelayReplication = data.shared.delayReplicationTime > 0;
    const delayHumane = genUtils.formatTimeSpan(1000 * (data.shared.delayReplicationTime ?? 0), true);
    const connectionStringDefined = !!data.shared.destinationDatabase;
    const { appUrl } = useAppUrls();
    const connectionStringsUrl = appUrl.forConnectionStrings(db, "Raven", data.shared.connectionStringName);

    return (
        <RichPanelDetails>
            {showDelayReplication && (
                <RichPanelDetailItem>
                    Replication Delay Time:
                    <div className="value">{delayHumane}</div>
                </RichPanelDetailItem>
            )}
            <ConnectionStringItem
                connectionStringDefined={!!data.shared.destinationDatabase}
                canEdit={canEdit}
                connectionStringName={data.shared.connectionStringName}
                connectionStringsUrl={connectionStringsUrl}
            />
            {connectionStringDefined && (
                <RichPanelDetailItem>
                    Destination Database:
                    <div className="value">{data.shared.destinationDatabase}</div>
                </RichPanelDetailItem>
            )}
            <RichPanelDetailItem>
                Actual Destination URL:
                <div className="value">
                    {data.shared.destinationUrl ? (
                        <a href={data.shared.destinationUrl} target="_blank">
                            {data.shared.destinationUrl}
                        </a>
                    ) : (
                        <div>N/A</div>
                    )}
                </div>
            </RichPanelDetailItem>
            {data.shared.topologyDiscoveryUrls?.length > 0 && (
                <RichPanelDetailItem>
                    Topology Discovery URLs:
                    <div className="value">{data.shared.topologyDiscoveryUrls.join(", ")}</div>
                </RichPanelDetailItem>
            )}
        </RichPanelDetails>
    );
}

export function ExternalReplicationPanel(props: ExternalReplicationPanelProps) {
    const { db, data } = props;

    const { isAdminAccessOrAbove } = useAccessManager();
    const { forCurrentDatabase } = useAppUrls();

    const canEdit = isAdminAccessOrAbove(db) && !data.shared.serverWide;
    const editUrl = forCurrentDatabase.editExternalReplication(data.shared.taskId)();

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

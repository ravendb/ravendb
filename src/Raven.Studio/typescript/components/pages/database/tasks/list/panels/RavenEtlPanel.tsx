import React from "react";
import { useAccessManager } from "hooks/useAccessManager";
import { RichPanel, RichPanelDetailItem, RichPanelDetails, RichPanelHeader } from "../../../../../common/RichPanel";
import { ConnectionStringItem, OngoingTaskActions, OngoingTaskName, OngoingTaskStatus } from "../shared";
import { useAppUrls } from "hooks/useAppUrls";
import { OngoingTaskRavenEtlInfo } from "../../../../../models/tasks";
import { BaseOngoingTaskPanelProps, useTasksOperations } from "../shared";

type RavenEtlPanelProps = BaseOngoingTaskPanelProps<OngoingTaskRavenEtlInfo>;

function Details(props: RavenEtlPanelProps & { canEdit: boolean }) {
    const { data, canEdit, db } = props;
    const connectionStringDefined = !!data.shared.destinationDatabase;
    const { appUrl } = useAppUrls();
    const connectionStringsUrl = appUrl.forConnectionStrings(db, "ravendb", data.shared.connectionStringName);
    //TODO: task status
    //TODO: progress template!

    return (
        <RichPanelDetails>
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

export function RavenEtlPanel(props: RavenEtlPanelProps) {
    const { db, data, ...restProps } = props;

    const { isAdminAccessOrAbove } = useAccessManager();
    const { forCurrentDatabase } = useAppUrls();

    const canEdit = isAdminAccessOrAbove(db) && !data.shared.serverWide;
    const editUrl = forCurrentDatabase.editRavenEtl(data.shared.taskId)();

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

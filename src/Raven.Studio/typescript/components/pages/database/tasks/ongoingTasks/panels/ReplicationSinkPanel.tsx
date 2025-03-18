import React from "react";
import {
    BaseOngoingTaskPanelProps,
    ConnectionStringItem,
    OngoingTaskActions,
    OngoingTaskName,
    OngoingTaskResponsibleNode,
    OngoingTaskStatus,
    useTasksOperations,
} from "../../shared/shared";
import { OngoingTaskReplicationSinkInfo } from "components/models/tasks";
import { useAppUrls } from "hooks/useAppUrls";
import {
    RichPanel,
    RichPanelActions,
    RichPanelDetailItem,
    RichPanelDetails,
    RichPanelHeader,
    RichPanelInfo,
    RichPanelSelect,
} from "components/common/RichPanel";
import Collapse from "react-bootstrap/Collapse";
import Form from "react-bootstrap/Form";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppSelector } from "components/store";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { ExternalReplicationTaskDistribution } from "components/pages/database/tasks/ongoingTasks/partials/ExternalReplicationTaskDistribution";

type ReplicationSinkPanelProps = BaseOngoingTaskPanelProps<OngoingTaskReplicationSinkInfo>;

function Details(props: ReplicationSinkPanelProps & { canEdit: boolean }) {
    const { data, canEdit } = props;
    const connectionStringDefined = !!data.shared.destinationDatabase;
    const { appUrl } = useAppUrls();

    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const connectionStringsUrl = appUrl.forConnectionStrings(databaseName, "Raven", data.shared.connectionStringName);

    return (
        <RichPanelDetails>
            <RichPanelDetailItem label="Hub Name">{data.shared.hubName}</RichPanelDetailItem>
            <ConnectionStringItem
                connectionStringDefined={connectionStringDefined}
                canEdit={canEdit}
                connectionStringName={data.shared.connectionStringName}
                connectionStringsUrl={connectionStringsUrl}
            />
            {data.shared.destinationDatabase && (
                <RichPanelDetailItem label="Hub Database">{data.shared.destinationDatabase}</RichPanelDetailItem>
            )}
            <RichPanelDetailItem label="Actual Hub URL">{data.shared.destinationUrl ?? "N/A"}</RichPanelDetailItem>

            {data.shared.topologyDiscoveryUrls.map((url) => (
                <RichPanelDetailItem label="Topology Discovery URL" key={url}>
                    {url}
                </RichPanelDetailItem>
            ))}
        </RichPanelDetails>
    );
}

export function ReplicationSinkPanel(props: ReplicationSinkPanelProps) {
    const { data, toggleSelection, isSelected, onTaskOperation, isDeleting, isTogglingState } = props;

    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();
    const canEdit = hasDatabaseAdminAccess && !data.shared.serverWide;

    const { forCurrentDatabase } = useAppUrls();
    const editUrl = forCurrentDatabase.editReplicationSink(data.shared.taskId)();

    const { detailsVisible, toggleDetails, onEdit } = useTasksOperations(editUrl, props);

    return (
        <RichPanel>
            <RichPanelHeader>
                <RichPanelInfo>
                    {canEdit && (
                        <RichPanelSelect>
                            <Form.Check
                                type="checkbox"
                                onChange={(e) => toggleSelection(e.currentTarget.checked, data.shared)}
                                checked={isSelected(data.shared.taskId)}
                            />
                        </RichPanelSelect>
                    )}
                    <OngoingTaskName task={data} canEdit={canEdit} editUrl={editUrl} />
                </RichPanelInfo>
                <RichPanelActions>
                    <OngoingTaskResponsibleNode task={data} />
                    <OngoingTaskStatus
                        task={data}
                        canEdit={canEdit}
                        onTaskOperation={onTaskOperation}
                        isTogglingState={isTogglingState(data.shared.taskId)}
                    />
                    <OngoingTaskActions
                        task={data}
                        canEdit={canEdit}
                        onEdit={onEdit}
                        onTaskOperation={onTaskOperation}
                        toggleDetails={toggleDetails}
                        isDeleting={isDeleting(data.shared.taskId)}
                    />
                </RichPanelActions>
            </RichPanelHeader>
            <Collapse in={detailsVisible}>
                <div>
                    <Details {...props} canEdit={canEdit} />
                    <ExternalReplicationTaskDistribution task={data} />
                </div>
            </Collapse>
        </RichPanel>
    );
}

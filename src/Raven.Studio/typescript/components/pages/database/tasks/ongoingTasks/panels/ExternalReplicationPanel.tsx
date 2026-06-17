import React from "react";
import { OngoingTaskExternalReplicationInfo } from "components/models/tasks";
import {
    RichPanel,
    RichPanelActions,
    RichPanelDetailItem,
    RichPanelDetails,
    RichPanelHeader,
    RichPanelInfo,
    RichPanelSelect,
} from "components/common/RichPanel";
import {
    ConnectionStringItem,
    DestinationUrlItem,
    OngoingTaskActions,
    OngoingTaskName,
    OngoingTaskResponsibleNode,
    OngoingTaskStatus,
} from "../../shared/shared";
import { useAppUrls } from "hooks/useAppUrls";
import { BaseOngoingTaskPanelProps, useTasksOperations } from "../../shared/shared";
import genUtils from "common/generalUtils";
import Collapse from "react-bootstrap/Collapse";
import Form from "react-bootstrap/Form";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppSelector } from "components/store";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { ExternalReplicationTaskDistribution } from "components/pages/database/tasks/ongoingTasks/partials/ExternalReplicationTaskDistribution";
import { Icon } from "components/common/Icon";

type ExternalReplicationPanelProps = BaseOngoingTaskPanelProps<OngoingTaskExternalReplicationInfo>;

function Details(props: ExternalReplicationPanelProps & { canEdit: boolean }) {
    const { data, canEdit } = props;

    const showDelayReplication = data.shared.delayReplicationTime > 0;
    const delayHumane = genUtils.formatTimeSpan(1000 * (data.shared.delayReplicationTime ?? 0), true);
    const connectionStringDefined = !!data.shared.destinationDatabase;
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    return (
        <RichPanelDetails>
            {showDelayReplication && (
                <RichPanelDetailItem label="Replication Delay Time">{delayHumane}</RichPanelDetailItem>
            )}
            <ConnectionStringItem
                connectionStringDefined={!!data.shared.destinationDatabase}
                canEdit={canEdit}
                connectionStringName={data.shared.connectionStringName}
                connectionStringType="Raven"
                databaseName={databaseName}
            />
            {connectionStringDefined && (
                <RichPanelDetailItem label="Destination Database">
                    {data.shared.destinationDatabase}
                </RichPanelDetailItem>
            )}
            <DestinationUrlItem destinationUrl={data.shared.destinationUrl} label="Actual Destination URL" />
            {data.shared.topologyDiscoveryUrls?.length > 0 && (
                <RichPanelDetailItem label="Topology Discovery URLs">
                    {data.shared.topologyDiscoveryUrls.join(", ")}
                </RichPanelDetailItem>
            )}
        </RichPanelDetails>
    );
}

export function ExternalReplicationPanel(props: ExternalReplicationPanelProps) {
    const { data, toggleSelection, isSelected, onTaskOperation, isDeleting, isTogglingState } = props;

    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();
    const { forCurrentDatabase } = useAppUrls();

    const canEdit = hasDatabaseAdminAccess && !data.shared.serverWide;
    console.log("maxym data.shared", data.shared);

    const editUrl = forCurrentDatabase.editExternalReplication(data.shared.taskId)();

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
                    <span>
                        <Icon icon="external-replication" />
                        External Replication
                    </span>
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
                        isDetailsOpen={detailsVisible}
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

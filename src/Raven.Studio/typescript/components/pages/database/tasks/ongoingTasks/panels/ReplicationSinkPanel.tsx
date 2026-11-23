import React from "react";
import {
    BaseOngoingTaskPanelProps,
    ConnectionStringItem,
    DestinationUrlItem,
    formatReplicationMode,
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
import { Icon } from "components/common/Icon";
import { ReplicationSummaryItems } from "components/pages/database/tasks/ongoingTasks/partials/ReplicationSummaryItems";
import { DetailsToggleProps, TaskPanelToggleButton } from "./EtlPanelComponents";
import { ReplicationCursorDetailItem } from "components/pages/database/tasks/ongoingTasks/partials/ReplicationCursorDetailItem";

type ReplicationSinkPanelProps = BaseOngoingTaskPanelProps<OngoingTaskReplicationSinkInfo>;

function Summary(props: ReplicationSinkPanelProps & DetailsToggleProps) {
    const { data, detailsVisible, toggleDetails } = props;

    return (
        <RichPanelDetails>
            <TaskPanelToggleButton detailsVisible={detailsVisible} toggleDetails={toggleDetails} />
            <RichPanelDetailItem label="Hub Name" title={data.shared.hubName}>
                <div className="detail-item-value-truncate">{data.shared.hubName}</div>
            </RichPanelDetailItem>
            {data.shared.destinationDatabase && (
                <RichPanelDetailItem label="Hub Database" title={data.shared.destinationDatabase}>
                    <div className="detail-item-value-truncate">{data.shared.destinationDatabase}</div>
                </RichPanelDetailItem>
            )}
            <ReplicationSummaryItems task={data} />
        </RichPanelDetails>
    );
}

function Settings(props: ReplicationSinkPanelProps & { canEdit: boolean }) {
    const { data, canEdit } = props;
    const connectionStringDefined = !!data.shared.destinationDatabase;
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    const mode = formatReplicationMode(data.shared.mode);

    return (
        <RichPanelDetails>
            {mode && <RichPanelDetailItem label="Mode">{mode}</RichPanelDetailItem>}
            <ConnectionStringItem
                connectionStringDefined={connectionStringDefined}
                canEdit={canEdit}
                connectionStringName={data.shared.connectionStringName}
                connectionStringType="Raven"
                databaseName={databaseName}
            />
            <DestinationUrlItem destinationUrl={data.shared.destinationUrl} label="Actual Hub URL" />

            {data.shared.topologyDiscoveryUrls.map((url) => (
                <RichPanelDetailItem label="Topology Discovery URL" key={url}>
                    {url}
                </RichPanelDetailItem>
            ))}

            <ReplicationCursorDetailItem label="Hub Cursor" cursor={data.shared.hubCursor} />
            <ReplicationCursorDetailItem label="Sink Cursor" cursor={data.shared.sinkCursor} />
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
                    <span>
                        <Icon icon="pull-replication-agent" />
                        Replication Sink
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
                        isDeleting={isDeleting(data.shared.taskId)}
                    />
                </RichPanelActions>
            </RichPanelHeader>
            <Summary {...props} detailsVisible={detailsVisible} toggleDetails={toggleDetails} />
            <Collapse in={detailsVisible}>
                <div>
                    <Settings {...props} canEdit={canEdit} />
                    <ExternalReplicationTaskDistribution task={data} />
                </div>
            </Collapse>
        </RichPanel>
    );
}

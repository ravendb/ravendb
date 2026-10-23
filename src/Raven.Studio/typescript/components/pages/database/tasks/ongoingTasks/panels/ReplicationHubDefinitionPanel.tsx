import React from "react";
import {
    BaseOngoingTaskPanelProps,
    formatReplicationMode,
    OngoingTaskActions,
    OngoingTaskName,
    OngoingTaskStatus,
    useTasksOperations,
} from "../../shared/shared";
import { OngoingTaskHubDefinitionInfo, OngoingTaskReplicationHubInfo } from "components/models/tasks";
import {
    RichPanel,
    RichPanelActions,
    RichPanelDetailItem,
    RichPanelDetails,
    RichPanelHeader,
    RichPanelInfo,
    RichPanelSelect,
} from "components/common/RichPanel";
import { useAppUrls } from "hooks/useAppUrls";
import { ReplicationHubConnectedSinkPanel } from "./ReplicationHubConnectedSinkPanel";
import genUtils from "common/generalUtils";
import Collapse from "react-bootstrap/Collapse";
import Form from "react-bootstrap/Form";
import { EmptySet } from "components/common/EmptySet";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { useAppSelector } from "components/store";
import { Icon } from "components/common/Icon";
import { ReplicationHubSinksHeader } from "../partials/ReplicationHubSinksHeader";
import { getSinkKey, useReplicationHubSinks } from "../partials/useReplicationHubSinks";

interface ReplicationHubPanelProps extends BaseOngoingTaskPanelProps<OngoingTaskHubDefinitionInfo> {
    connectedSinks: OngoingTaskReplicationHubInfo[];
}

function Details(props: ReplicationHubPanelProps & { canEdit: boolean }) {
    const { connectedSinks, data } = props;
    const sinksState = useReplicationHubSinks(connectedSinks);
    const { visibleSinks, expandedSinkKeys, toggleSinkExpanded } = sinksState;

    const delayHumane = data.shared.delayReplicationTime
        ? genUtils.formatTimeSpan(data.shared.delayReplicationTime * 1000, true)
        : null;

    return (
        <div>
            <RichPanelDetails>
                {delayHumane && <RichPanelDetailItem label="Replication Delay Time">{delayHumane}</RichPanelDetailItem>}
                <RichPanelDetailItem label="Replication Mode">
                    {formatReplicationMode(data.shared.taskMode)}
                </RichPanelDetailItem>
                <RichPanelDetailItem label="Has Filtering">
                    {data.shared.hasFiltering ? "True" : "False"}
                </RichPanelDetailItem>
            </RichPanelDetails>
            {connectedSinks.length > 0 && (
                <div className="replication-hub-sinks mx-3 mb-3">
                    <hr className="mt-0" />
                    <ReplicationHubSinksHeader sinksState={sinksState} />
                    {visibleSinks.map((sink) => {
                        const sinkKey = getSinkKey(sink);
                        return (
                            <ReplicationHubConnectedSinkPanel
                                key={sinkKey}
                                data={sink}
                                isExpanded={expandedSinkKeys.includes(sinkKey)}
                                toggleExpanded={() => toggleSinkExpanded(sinkKey)}
                            />
                        );
                    })}
                    {visibleSinks.length === 0 && <EmptySet>No sinks match the selected filters</EmptySet>}
                </div>
            )}
            <div className="margin-left">{connectedSinks.length === 0 && <EmptySet>No sinks connected</EmptySet>}</div>
        </div>
    );
}

export function ReplicationHubDefinitionPanel(props: ReplicationHubPanelProps) {
    const { data, toggleSelection, isSelected, onTaskOperation, isDeleting, isTogglingState } = props;

    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();

    const { forCurrentDatabase } = useAppUrls();
    const editUrl = forCurrentDatabase.editReplicationHub(data.shared.taskId)();
    const canEdit = hasDatabaseAdminAccess && !data.shared.serverWide;
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
                        <Icon icon="pull-replication-hub" />
                        Replication Hub
                    </span>
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
                </div>
            </Collapse>
        </RichPanel>
    );
}

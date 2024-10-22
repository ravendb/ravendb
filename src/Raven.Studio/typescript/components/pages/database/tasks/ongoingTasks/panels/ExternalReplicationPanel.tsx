import React, { useState } from "react";
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
    OngoingTaskActions,
    OngoingTaskName,
    OngoingTaskResponsibleNode,
    OngoingTaskStatus,
} from "../../shared/shared";
import { useAppUrls } from "hooks/useAppUrls";
import { BaseOngoingTaskPanelProps, useTasksOperations } from "../../shared/shared";
import genUtils from "common/generalUtils";
import { Button, Collapse, Input, Table } from "reactstrap";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppSelector } from "components/store";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import changeVectorUtils from "common/changeVectorUtils";
import { PopoverWithHover } from "components/common/PopoverWithHover";
import { Icon } from "components/common/Icon";
import copyToClipboard from "common/copyToClipboard";
import { ReplicationTaskDistribution } from "components/pages/database/tasks/ongoingTasks/panels/ReplicationTaskDistribution";

type ExternalReplicationPanelProps = BaseOngoingTaskPanelProps<OngoingTaskExternalReplicationInfo>;

function Details(props: ExternalReplicationPanelProps & { canEdit: boolean }) {
    const { data, canEdit } = props;

    const showDelayReplication = data.shared.delayReplicationTime > 0;
    const delayHumane = genUtils.formatTimeSpan(1000 * (data.shared.delayReplicationTime ?? 0), true);
    const connectionStringDefined = !!data.shared.destinationDatabase;
    const { appUrl } = useAppUrls();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const connectionStringsUrl = appUrl.forConnectionStrings(databaseName, "Raven", data.shared.connectionStringName);
    const [valuePopover, setValuePopover] = useState<HTMLElement>();

    return (
        <RichPanelDetails>
            {showDelayReplication && (
                <RichPanelDetailItem label="Replication Delay Time">{delayHumane}</RichPanelDetailItem>
            )}
            <ConnectionStringItem
                connectionStringDefined={!!data.shared.destinationDatabase}
                canEdit={canEdit}
                connectionStringName={data.shared.connectionStringName}
                connectionStringsUrl={connectionStringsUrl}
            />
            {connectionStringDefined && (
                <RichPanelDetailItem label="Destination Database">
                    {data.shared.destinationDatabase}
                </RichPanelDetailItem>
            )}
            <RichPanelDetailItem label="Actual Destination URL">
                {data.shared.destinationUrl ? (
                    <a href={data.shared.destinationUrl} target="_blank">
                        {data.shared.destinationUrl}
                    </a>
                ) : (
                    <div>N/A</div>
                )}
            </RichPanelDetailItem>
            {data.shared.topologyDiscoveryUrls?.length > 0 && (
                <RichPanelDetailItem label="Topology Discovery URLs">
                    {data.shared.topologyDiscoveryUrls.join(", ")}
                </RichPanelDetailItem>
            )}
            <PopoverWithHover target={valuePopover} placement="top">
                <ChangeVectorDetails
                    lastAcceptedChangeVectorFromDestination={data.shared.lastAcceptedChangeVectorFromDestination}
                    sourceDatabaseChangeVector={data.shared.sourceDatabaseChangeVector}
                />
            </PopoverWithHover>
        </RichPanelDetails>
    );
}

interface ChangeVectorDetailsProps {
    sourceDatabaseChangeVector: string;
    lastAcceptedChangeVectorFromDestination: string;
}

function ChangeVectorDetails(props: ChangeVectorDetailsProps) {
    const { sourceDatabaseChangeVector, lastAcceptedChangeVectorFromDestination } = props;

    const handleCopyToClipboard = (value: string) => {
        copyToClipboard.copy(value, "Item has been copied to clipboard");
    };

    const sourceDatabaseChangeVectorFormatted = sourceDatabaseChangeVector
        ? changeVectorUtils.formatChangeVector(sourceDatabaseChangeVector, true)
        : null;
    const lastAcceptedChangeVectorFromDestinationFormatted = lastAcceptedChangeVectorFromDestination
        ? changeVectorUtils.formatChangeVector(lastAcceptedChangeVectorFromDestination, true)
        : null;

    return (
        <div className="p-2">
            <Table>
                <tbody>
                    {sourceDatabaseChangeVectorFormatted && (
                        <tr>
                            <td>Source database CV</td>
                            <td>
                                <div className="d-flex gap-1">
                                    <div>
                                        {sourceDatabaseChangeVectorFormatted.map((x) => x.shortFormat).join(", ")}
                                    </div>
                                    <Button
                                        onClick={() =>
                                            handleCopyToClipboard(
                                                sourceDatabaseChangeVectorFormatted.map((x) => x.fullFormat).join(",")
                                            )
                                        }
                                        color="primary"
                                        size="sm"
                                        title="Copy to clipboard"
                                    >
                                        <Icon icon="copy-to-clipboard" margin="m-0" />
                                    </Button>
                                </div>
                            </td>
                        </tr>
                    )}

                    {lastAcceptedChangeVectorFromDestinationFormatted && (
                        <tr>
                            <td>Last accepted CV (from destination)</td>
                            <td>
                                <div className="d-flex gap-1">
                                    <div>
                                        {lastAcceptedChangeVectorFromDestinationFormatted
                                            .map((x) => x.shortFormat)
                                            .join(", ")}
                                    </div>
                                    <Button
                                        onClick={() =>
                                            handleCopyToClipboard(
                                                lastAcceptedChangeVectorFromDestinationFormatted
                                                    .map((x) => x.fullFormat)
                                                    .join(",")
                                            )
                                        }
                                        color="primary"
                                        size="sm"
                                        title="Copy to clipboard"
                                    >
                                        <Icon icon="copy-to-clipboard" margin="m-0" />
                                    </Button>
                                </div>
                            </td>
                        </tr>
                    )}
                </tbody>
            </Table>
        </div>
    );
}

export function ExternalReplicationPanel(props: ExternalReplicationPanelProps) {
    const { data, toggleSelection, isSelected, onTaskOperation, isDeleting, isTogglingState } = props;

    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();
    const { forCurrentDatabase } = useAppUrls();

    const canEdit = hasDatabaseAdminAccess && !data.shared.serverWide;
    const editUrl = forCurrentDatabase.editExternalReplication(data.shared.taskId)();

    const { detailsVisible, toggleDetails, onEdit } = useTasksOperations(editUrl, props);

    return (
        <RichPanel>
            <RichPanelHeader>
                <RichPanelInfo>
                    {canEdit && (
                        <RichPanelSelect>
                            <Input
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
            <Collapse isOpen={detailsVisible}>
                <Details {...props} canEdit={canEdit} />
                <ReplicationTaskDistribution task={data} />
            </Collapse>
        </RichPanel>
    );
}

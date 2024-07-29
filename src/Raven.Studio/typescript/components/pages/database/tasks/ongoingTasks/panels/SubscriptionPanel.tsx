import React, { useState } from "react";
import {
    RichPanel,
    RichPanelActions,
    RichPanelDetailItem,
    RichPanelDetails,
    RichPanelHeader,
    RichPanelInfo,
    RichPanelSelect,
} from "components/common/RichPanel";
import { OngoingTaskSubscriptionInfo, OngoingTaskSubscriptionSharedInfo } from "components/models/tasks";
import {
    BaseOngoingTaskPanelProps,
    OngoingTaskActions,
    OngoingTaskName,
    OngoingTaskResponsibleNode,
    OngoingTaskStatus,
    useTasksOperations,
} from "../../shared/shared";
import { useAppUrls } from "hooks/useAppUrls";
import { SubscriptionTaskDistribution } from "./SubscriptionTaskDistribution";
import genUtils from "common/generalUtils";
import moment from "moment";
import { Alert, Button, Collapse, Input } from "reactstrap";
import { PopoverWithHover } from "components/common/PopoverWithHover";
import { FlexGrow } from "components/common/FlexGrow";
import { Icon } from "components/common/Icon";
import { SubscriptionConnectionsDetailsWithId } from "../OngoingTasksReducer";
import { useAppSelector } from "components/store";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";

type SubscriptionPanelProps = BaseOngoingTaskPanelProps<OngoingTaskSubscriptionInfo> & {
    refreshSubscriptionInfo: () => void;
    connections: SubscriptionConnectionsDetailsWithId | undefined;
    dropSubscription: (workerId?: string) => void;
};

interface ChangeVectorInfoProps {
    info: OngoingTaskSubscriptionSharedInfo;
}

function ChangeVectorInfo(props: ChangeVectorInfoProps) {
    const { info } = props;

    if (
        info.changeVectorForNextBatchStartingPointPerShard &&
        Object.keys(info.changeVectorForNextBatchStartingPointPerShard).length > 0
    ) {
        return (
            <div className="p-3 change-vector-popover">
                <div className="change-vector-grid mb-1">
                    <strong>Shard</strong>
                    <strong>Change vector</strong>
                </div>
                {Object.keys(info.changeVectorForNextBatchStartingPointPerShard).map((shard) => {
                    const vector = info.changeVectorForNextBatchStartingPointPerShard[shard];
                    const vectorItems = vector.split(",");
                    return (
                        <div key={shard} className="change-vector-grid">
                            <div className="change-vector-label">
                                <Icon icon="shard" color="shard" className="m-0" />
                                <strong>#{shard}</strong>
                            </div>
                            <div className="change-vector-item">
                                {vectorItems.map((item, index) => (
                                    <React.Fragment key={index}>
                                        {index > 0 && (
                                            <span>
                                                ,<br />
                                            </span>
                                        )}
                                        {item.trim()}
                                    </React.Fragment>
                                ))}
                            </div>
                        </div>
                    );
                })}
            </div>
        );
    }

    if (info.changeVectorForNextBatchStartingPoint) {
        const vectorTrim = info.changeVectorForNextBatchStartingPoint.split(",");
        return (
            <div className="p-3 change-vector-popover">
                <div className="change-vector-item">
                    {vectorTrim.map((item, index) => (
                        <React.Fragment key={index}>
                            {index > 0 && (
                                <span>
                                    ,<br />
                                </span>
                            )}
                            {item.trim()}
                        </React.Fragment>
                    ))}
                </div>
            </div>
        );
    }

    return (
        <div className="p-3 change-vector-popover">
            <div className="change-vector-item">not yet available</div>
        </div>
    );
}

function Details(props: SubscriptionPanelProps) {
    const { data, refreshSubscriptionInfo } = props;

    const lastBatchAckTime = data.shared.lastBatchAckTime
        ? moment.utc(data.shared.lastBatchAckTime).local().format(genUtils.dateFormat)
        : "N/A";

    const lastClientConnectionTime = data.shared.lastClientConnectionTime
        ? moment.utc(data.shared.lastClientConnectionTime).local().format(genUtils.dateFormat)
        : "N/A";

    const [changeVectorInfoElement, setChangeVectorInfoElement] = useState<HTMLElement>();

    return (
        <RichPanelDetails>
            <RichPanelDetailItem label="Last Batch Ack Time">{lastBatchAckTime}</RichPanelDetailItem>
            <RichPanelDetailItem label="Last Client Connection Time">{lastClientConnectionTime}</RichPanelDetailItem>
            <RichPanelDetailItem
                label="Change vector for next batch"
                className="d-flex flex-horizontal align-self-baseline"
            >
                <i ref={setChangeVectorInfoElement} className="icon-info text-info ms-1"></i>
                {changeVectorInfoElement && (
                    <PopoverWithHover target={changeVectorInfoElement}>
                        <ChangeVectorInfo info={data.shared} />
                    </PopoverWithHover>
                )}
            </RichPanelDetailItem>
            <FlexGrow />
            <div>
                <Button onClick={refreshSubscriptionInfo}>
                    <Icon icon="refresh" />
                    Refresh
                </Button>
            </div>
        </RichPanelDetails>
    );
}

interface ConnectedClientsProps {
    connections: SubscriptionConnectionsDetailsWithId;
    dropSubscription: (workerId?: string) => void;
    refreshSubscriptionInfo: () => void;
}

function ConnectedClients(props: ConnectedClientsProps) {
    const { connections, dropSubscription, refreshSubscriptionInfo } = props;

    if (!connections) {
        return null;
    }

    if (connections.LoadError) {
        return <Alert color="warning">{connections.LoadError}</Alert>;
    }

    const disconnectSubscription = async (workerId: string) => {
        try {
            await dropSubscription(workerId);
        } finally {
            await refreshSubscriptionInfo();
        }
    };

    return (
        <div className="m-3 p-2 connected-clients-section">
            <h3>Connected clients</h3>
            {connections.Results.length > 0 && (
                <div className="mb-2">
                    <div className="small-label">Subscription Mode</div>
                    <div>{connections.SubscriptionMode}</div>
                </div>
            )}
            <div className="connected-clients-items">
                {connections.Results.length === 0 && <div>No clients connected</div>}
                {connections.Results.map((connection) => (
                    <div key={connection.ClientUri} className="text-center py-2 connected-client">
                        <div className="pb-1">
                            <Icon icon="client" color="primary" className="mb-1" />
                        </div>
                        <div className="p-2">
                            <div className="small-label">Connection Strategy</div>
                            <span>{connection.Strategy}</span>
                        </div>
                        <div className="p-2">
                            <div className="small-label">Client URI</div>
                            <span>{connection.ClientUri}</span>
                        </div>
                        <div>
                            <Button
                                color="danger"
                                title="Disconnect client from this subscription (unsubscribe client)"
                                onClick={() => disconnectSubscription(connection.WorkerId)}
                                size="xs"
                                className="rounded-pill mt-2"
                                outline
                            >
                                <Icon icon="disconnected" />
                                Disconnect
                            </Button>
                        </div>
                    </div>
                ))}
            </div>
        </div>
    );
}

export function SubscriptionPanel(props: SubscriptionPanelProps) {
    const {
        data,
        connections,
        dropSubscription,
        refreshSubscriptionInfo,
        toggleSelection,
        isSelected,
        onTaskOperation,
        isDeleting,
        isTogglingState,
    } = props;

    const hasDatabaseWriteAccess = useAppSelector(accessManagerSelectors.getHasDatabaseWriteAccess)();
    const { forCurrentDatabase } = useAppUrls();

    const canEdit = hasDatabaseWriteAccess && !data.shared.serverWide;
    const editUrl = forCurrentDatabase.editSubscription(data.shared.taskId, data.shared.taskName)();

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
                <Details {...props} />
                <SubscriptionTaskDistribution task={data} />
                <ConnectedClients
                    dropSubscription={dropSubscription}
                    refreshSubscriptionInfo={refreshSubscriptionInfo}
                    connections={connections}
                />
            </Collapse>
        </RichPanel>
    );
}

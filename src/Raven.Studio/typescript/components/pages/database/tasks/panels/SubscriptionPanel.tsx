import React, { useState } from "react";
import {
    RichPanel,
    RichPanelActions,
    RichPanelDetailItem,
    RichPanelDetails,
    RichPanelHeader,
    RichPanelInfo,
} from "components/common/RichPanel";
import { OngoingTaskSubscriptionInfo, OngoingTaskSubscriptionSharedInfo } from "components/models/tasks";
import {
    BaseOngoingTaskPanelProps,
    OngoingTaskActions,
    OngoingTaskName,
    OngoingTaskResponsibleNode,
    OngoingTaskStatus,
    useTasksOperations,
} from "../shared";
import { useAccessManager } from "hooks/useAccessManager";
import { useAppUrls } from "hooks/useAppUrls";
import { SubscriptionTaskDistribution } from "./SubscriptionTaskDistribution";
import genUtils from "common/generalUtils";
import moment from "moment";
import { Alert, Button, Collapse } from "reactstrap";
import { PopoverWithHover } from "components/common/PopoverWithHover";
import { FlexGrow } from "components/common/FlexGrow";
import { SubscriptionConnectionsDetailsWithId } from "components/pages/database/tasks/list/OngoingTasksReducer";
import { Icon } from "components/common/Icon";
import { EmptySet } from "components/common/EmptySet";

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

    /* TODO work on UI
      for non-sharded dbs: we have single change vector for next batch
      for sharded: we have change vector per each shard!
     */

    //TODO: can we have both fields filled in?

    if (info.changeVectorForNextBatchStartingPoint) {
        return (
            <div className="p-3 change-vector-popover">
                <div className="change-vector-item">{info.changeVectorForNextBatchStartingPoint}</div>
            </div>
        );
    }

    if (!info.changeVectorForNextBatchStartingPointPerShard) {
        return (
            <div className="p-3 change-vector-popover">
                <div className="change-vector-item">N/A</div>
            </div>
        );
    }

    return (
        <div className="p-3 change-vector-popover">
            <div className="change-vector-grid mb-1">
                <strong>Shard</strong>
                <strong>Change vector</strong>
            </div>
            {Object.keys(info.changeVectorForNextBatchStartingPointPerShard).map((shard) => {
                const vector = info.changeVectorForNextBatchStartingPointPerShard[shard];
                return (
                    <div key={shard} className="change-vector-grid">
                        <div>
                            <Icon icon="shard" color="shard" className="m-0" />
                            <strong>#{shard}</strong>
                        </div>
                        <div className="change-vector-item">{vector}</div>
                    </div>
                );
            })}
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

    //TODO: do we need proxy for that?
    const disconnectSubscription = async (workerId: string) => {
        try {
            await dropSubscription(workerId);
        } finally {
            await refreshSubscriptionInfo();
        }
    };

    //TODO: create L&F for connections section!

    return (
        <div className="m-3 p-2 connected-clients-section">
            <h4>Connected clients - Subscription Mode: {connections.SubscriptionMode}</h4>
            <div className="connected-clients-items">
                {connections.Results.length === 0 && <EmptySet>No clients connected</EmptySet>}
                {connections.Results.map((connection) => (
                    <div className="text-center py-2 connected-client">
                        <div className="pb-1">
                            <Icon icon="client" color="primary" className="mb-1" />
                        </div>
                        <div className="p-2">
                            <div className="small-label">Connection Strategy</div>
                            <span>{connection.Strategy}</span>
                        </div>
                        <div className="p-2">
                            <div className="small-label">Client URI</div>
                            <a href={connection.ClientUri} className="no-decor">
                                {connection.ClientUri}
                            </a>
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
    const { db, data, connections, dropSubscription, refreshSubscriptionInfo } = props;

    const { canReadWriteDatabase } = useAccessManager();
    const { forCurrentDatabase } = useAppUrls();

    const canEdit = canReadWriteDatabase(db) && !data.shared.serverWide;
    const editUrl = forCurrentDatabase.editSubscription(data.shared.taskId, data.shared.taskName)();

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
                    <OngoingTaskResponsibleNode task={data} />
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

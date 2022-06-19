import React, { useCallback } from "react";
import { RichPanel, RichPanelDetails, RichPanelHeader } from "../../../../../common/RichPanel";
import { OngoingTaskSubscriptionInfo } from "../../../../../models/tasks";
import {
    BaseOngoingTaskPanelProps,
    OngoingTaskActions,
    OngoingTaskName,
    OngoingTaskStatus,
    useTasksOperations,
} from "../shared";
import { useAccessManager } from "hooks/useAccessManager";
import { useAppUrls } from "hooks/useAppUrls";
import { OngoingEtlTaskDistribution } from "./OngoingEtlTaskDistribution";
import { SubscriptionTaskDistribution } from "./SubscriptionTaskDistribution";

type SubscriptionPanelProps = BaseOngoingTaskPanelProps<OngoingTaskSubscriptionInfo>;

function Details(props: SubscriptionPanelProps) {
    return <h3>details</h3>;
}

export function SubscriptionPanel(props: SubscriptionPanelProps) {
    const { db, data } = props;

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
            {detailsVisible && <Details {...props} />}
            {detailsVisible && <SubscriptionTaskDistribution task={data} />}
        </RichPanel>
    );

    //tODO:

    /*
    return (
            <div className="collapse panel-addon" data-bind="collapse: showDetails">
                <div className="padding-sm flex-horizontal flex-wrap">
                    <div>
                        <div className="list-properties">
                        
                            <div data-bind="foreach: clients">
                                <div className="property-item">
                                    <div className="property-name">Client URI:</div>
                                    <div className="property-value text-details">
                                        <div className="flex-horizontal">
                                            <div data-bind="text: clientUri"></div>
                                            <button
                                                className="btn btn-xs margin-left margin-left-sm"
                                                data-bind="click: _.partial($parent.disconnectClientFromSubscription, workerId)"
                                                title="Disconnect client from this subscription (unsubscribe client)"
                                            >
                                                <i className="icon-disconnected"></i>
                                                <span>Disconnect</span>
                                            </button>
                                        </div>
                                    </div>
                                </div>
                                <div className="property-item">
                                    <div className="property-name">Connection Strategy:</div>
                                    <div className="property-value text-details">
                                        <div className="flex-horizontal">
                                            <div data-bind="text: strategy"></div>
                                        </div>
                                    </div>
                                </div>
                                <hr />
                            </div>
                            /ko
                            <div className="property-item">
                                <div className="property-name">Change vector for next batch:</div>
                                <div
                                    className="property-value text-details"
                                    data-bind="text: changeVectorForNextBatchStartingPoint() || 'N/A'"
                                ></div>
                            </div>
                            <div className="property-item">
                                <div className="property-name">Last Batch Acknowledgement Time:</div>
                                <div
                                    className="property-value text-details"
                                    data-bind="text: lastTimeServerMadeProgressWithDocuments"
                                ></div>
                            </div>
                            <div className="property-item">
                                <div className="property-name">Last Client Connection Time:</div>
                                <div className="property-value">
                                    <span className="text-details" data-bind="text: lastClientConnectionTime"></span>
                                </div>
                            </div>
                        </div>
                    </div>
                    <div className="flex-noshrink flex-grow flex-start text-right">
                        <button
                            className="btn btn-default"
                            data-bind="click: refreshSubscriptionInfo"
                            title="Refresh info"
                        >
                            <i className="icon-refresh"></i>
                        </button>
                    </div>
                </div>
            </div>
        </div>
    );*/
}

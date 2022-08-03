import React from "react";
import { RichPanel, RichPanelDetailItem, RichPanelDetails, RichPanelHeader } from "../../../../../common/RichPanel";
import { OngoingTaskSubscriptionInfo } from "../../../../../models/tasks";
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

type SubscriptionPanelProps = BaseOngoingTaskPanelProps<OngoingTaskSubscriptionInfo>;

function Details(props: SubscriptionPanelProps) {
    const { data } = props;

    const lastBatchAckTime = data.shared.lastBatchAckTime
        ? moment.utc(data.shared.lastBatchAckTime).local().format(genUtils.dateFormat)
        : "N/A";

    const lastClientConnectionTime = data.shared.lastClientConnectionTime
        ? moment.utc(data.shared.lastClientConnectionTime).local().format(genUtils.dateFormat)
        : "N/A";

    //TODO looks like server doesn't send those 2 times correctly
    //TODO: tooltip for next change vector

    return (
        <RichPanelDetails>
            <RichPanelDetailItem>
                Last Batch Ack Time:
                <div className="value">{lastBatchAckTime}</div>
            </RichPanelDetailItem>
            <RichPanelDetailItem>
                Last Client Connection Time:
                <div className="value">{lastClientConnectionTime}</div>
            </RichPanelDetailItem>
            <RichPanelDetailItem>
                Change vector for next batch:
                <div className="value">
                    <i className="icon-info text-info"></i>
                </div>
            </RichPanelDetailItem>
        </RichPanelDetails>
    );
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
            {detailsVisible && <Details {...props} />}
            {detailsVisible && <SubscriptionTaskDistribution task={data} />}
        </RichPanel>
    );

    //tODO:

    /*
    fields:
    mode: subscripton mode (is it global)
    
    show client details issues?  - clientDetailsIssue
    
    check if we can query to find responsbile nodes and them fetch details only from them to avoid to many calls?
  
     */
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
        
        class ongoingTaskSubscriptionListModel extends ongoingTaskListModel {
    
    // General stats
    lastTimeServerMadeProgressWithDocuments = ko.observable<string>();

    // Live connection stats
    clients = ko.observableArray<PerConnectionStats>([]);
    clientDetailsIssue = ko.observable<string>(); // null (ok) | client is not connected | failed to get details..
    subscriptionMode = ko.observable<string>();
    textClass = ko.observable<string>("text-details");
    
    initializeObservables(): void {
        super.initializeObservables();
        this.taskState.subscribe(() => this.refreshIfNeeded());
    }

    toggleDetails(): void {
        this.showDetails.toggle();
        this.refreshIfNeeded()
    }
    
    private refreshIfNeeded(): void {
        if (this.showDetails()) {
            this.refreshSubscriptionInfo();
        }
    }

    private refreshSubscriptionInfo() {
        // 1. Get general info
        ongoingTaskInfoCommand.forSubscription(this.activeDatabase(), this.taskId, this.taskName())
            .execute()
            .done((result: Raven.Client.Documents.Subscriptions.SubscriptionStateWithNodeDetails) => {

                this.responsibleNode(result.ResponsibleNode);
                this.taskState(result.Disabled ? 'Disabled' : 'Enabled');
                
                // 2. Get connection details info
                this.clientDetailsIssue(null);
                new subscriptionConnectionDetailsCommand(this.activeDatabase(), this.taskId, this.taskName(), this.responsibleNode().NodeUrl)
                    .execute()
                    .done((result: Raven.Server.Documents.TcpHandlers.SubscriptionConnectionsDetails) => {

                        this.subscriptionMode(result.SubscriptionMode);
                        
                        this.clients(result.Results.map(x => ({
                            clientUri: x.ClientUri,
                            strategy: x.Strategy,
                            workerId: x.WorkerId
                        })));

                        if (!result.Results.length) { 
                            this.clientDetailsIssue("No client is connected");
                            this.textClass("text-warning");
                        }
                    })
                    .fail((response: JQueryXHR) => {
                        if (response.status === 0) {
                            // we can't even connect to node, show node connectivity error
                            this.clientDetailsIssue("Failed to connect to " + this.responsibleNode().NodeUrl + ". Please make sure this url is accessible from your browser.");
                        } else {
                            this.clientDetailsIssue("Failed to get client connection details");
                        }
                        
                        this.textClass("text-danger");
                    });
            });
    }

    disconnectClientFromSubscription(workerId: string) {
        new dropSubscriptionConnectionCommand(this.activeDatabase(), this.taskId, this.taskName(), workerId)
            .execute()
            .done(() => this.refreshSubscriptionInfo());
    }
}

export = ongoingTaskSubscriptionListModel;
    );*/
}

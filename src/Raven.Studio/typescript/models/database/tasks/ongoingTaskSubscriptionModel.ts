/// <reference path="../../../../typings/tsd.d.ts"/>
import appUrl = require("common/appUrl");
import router = require("plugins/router");
import ongoingTask = require("models/database/tasks/ongoingTaskModel");
import ongoingTaskInfoCommand = require("commands/database/tasks/getOngoingTaskInfoCommand");
import subscriptionConnectionDetailsCommand = require("commands/database/tasks/getSubscriptionConnectionDetailsCommand");
import dropSubscriptionConnectionCommand = require("commands/database/tasks/dropSubscriptionConnectionCommand");
import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");

// This model is used by the 'Ongoing Tasks List View'
class ongoingTaskSubscriptionModel extends ongoingTask {

    editUrl: KnockoutComputed<string>;
    activeDatabase = activeDatabaseTracker.default.database;

    // General stats
    lastTimeServerMadeProgressWithDocuments = ko.observable<string>();
    lastClientConnectionTime = ko.observable<string>();

    // Live connection stats
    clientIP = ko.observable<string>();
    connectionStrategy = ko.observable<Raven.Client.Documents.Subscriptions.SubscriptionOpeningStrategy>();  
    clientDetailsIssue = ko.observable<string>(); // null (ok) | client is not connected | failed to get details.. 
    textClass = ko.observable<string>("text-details");

    validationGroup: KnockoutValidationGroup; 
    showSubscriptionDetails = ko.observable(false);
    
    constructor(dto: Raven.Client.ServerWide.Operations.OngoingTaskSubscription | Raven.Client.Documents.Subscriptions.SubscriptionStateWithNodeDetails, isInListView: boolean) {
        super();

        this.isInTasksListView = isInListView;
        this.listViewUpdate(dto);
        this.listViewInitializeObservables(); 
    }

    listViewInitializeObservables() {
        super.initializeObservables();

        const urls = appUrl.forCurrentDatabase();
        this.editUrl = urls.editSubscription(this.taskId, this.taskName());
    }

    listViewUpdate(dto: Raven.Client.ServerWide.Operations.OngoingTaskSubscription | Raven.Client.Documents.Subscriptions.SubscriptionStateWithNodeDetails) {

        // 1. Must pass the right data in case we are in Edit View flow
        if ('Criteria' in dto) {
            const dtoEditModel = dto as Raven.Client.Documents.Subscriptions.SubscriptionStateWithNodeDetails;

            const state: Raven.Client.ServerWide.Operations.OngoingTaskState = dtoEditModel.Disabled ? 'Disabled' : 'Enabled';
            const emptyNodeId: Raven.Client.ServerWide.Operations.NodeId = { NodeTag: "", NodeUrl: "", ResponsibleNode: "" };
            
            const dtoListModel: Raven.Client.ServerWide.Operations.OngoingTaskSubscription = {
                ResponsibleNode: emptyNodeId,
                TaskConnectionStatus: 'Active', // todo: this has to be reviewed...
                TaskId: dtoEditModel.SubscriptionId,
                TaskName: dtoEditModel.SubscriptionName,
                TaskState: state,
                Query: dtoEditModel.Query,
                TaskType: 'Subscription',
                Error: null
            };

            super.update(dtoListModel);
        }
        // 2. List View flow
        else {
            super.update(dto as Raven.Client.ServerWide.Operations.OngoingTaskSubscription);
        }
    }

    editTask() {
        router.navigate(this.editUrl());
    }

    toggleDetails() {
        this.showSubscriptionDetails(!this.showSubscriptionDetails());

        if (this.showSubscriptionDetails()) {
            this.refreshSubscriptionInfo();
        }
    }

    refreshSubscriptionInfo() {
        // 1. Get general info
        ongoingTaskInfoCommand.forSubscription(this.activeDatabase(), this.taskId, this.taskName())
            .execute()
            .done((result: Raven.Client.Documents.Subscriptions.SubscriptionStateWithNodeDetails) => {

                this.responsibleNode(result.ResponsibleNode);
                this.taskState(result.Disabled ? 'Disabled' : 'Enabled');
                
                const dateFormat = "YYYY MMMM Do, h:mm A";
                const lastServerTime = moment.utc(result.LastTimeServerMadeProgressWithDocuments).local().format(dateFormat);
                this.lastTimeServerMadeProgressWithDocuments(lastServerTime);
                const lastClientTime = moment.utc(result.LastClientConnectionTime).local().format(dateFormat);
                this.lastClientConnectionTime(lastClientTime);

                // 2. Get connection details info
                this.clientDetailsIssue(null);
                new subscriptionConnectionDetailsCommand(this.activeDatabase(), this.taskId, this.taskName(), this.responsibleNode().NodeUrl)
                    .execute()
                    .done((result: Raven.Server.Documents.TcpHandlers.SubscriptionConnectionDetails) => {

                        this.clientIP(result.ClientUri);
                        this.connectionStrategy(result.Strategy);

                        if (!this.clientIP()) { 
                            this.clientDetailsIssue("No client is connected");
                            this.textClass("text-warning");
                        }
                    })
                    .fail(() => {
                        this.clientDetailsIssue("Failed to get client connection details");
                        this.textClass("text-danger");
                    });
            });
    }

    disconnectClientFromSubscription() {
        new dropSubscriptionConnectionCommand(this.activeDatabase(), this.taskId, this.taskName())
            .execute()
            .done(() => { this.refreshSubscriptionInfo(); });
    }
}

export = ongoingTaskSubscriptionModel;

/// <reference path="../../../../typings/tsd.d.ts"/>
import appUrl = require("common/appUrl");
import router = require("plugins/router");
import ongoingTaskListModel = require("models/database/tasks/ongoingTaskListModel");
import ongoingTaskInfoCommand = require("commands/database/tasks/getOngoingTaskInfoCommand");
import subscriptionConnectionDetailsCommand = require("commands/database/tasks/getSubscriptionConnectionDetailsCommand");
import dropSubscriptionConnectionCommand = require("commands/database/tasks/dropSubscriptionConnectionCommand");
import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");

class ongoingTaskSubscriptionListModel extends ongoingTaskListModel {

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
    showDetails = ko.observable(false);
    
    constructor(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskSubscription) {
        super();

        this.update(dto);
        this.initializeObservables(); 
    }

    initializeObservables() {
        super.initializeObservables();

        const urls = appUrl.forCurrentDatabase();
        this.editUrl = urls.editSubscription(this.taskId, this.taskName());
    }

    editTask() {
        router.navigate(this.editUrl());
    }

    toggleDetails() {
        this.showDetails.toggle();

        if (this.showDetails()) {
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

                const lastServerTime = (!!result.LastBatchAckTime) ? moment.utc(result.LastBatchAckTime).local().format(dateFormat):"N/A";
                this.lastTimeServerMadeProgressWithDocuments(lastServerTime);
                const lastClientTime = (!!result.LastClientConnectionTime)?moment.utc(result.LastClientConnectionTime).local().format(dateFormat):"N/A";
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

export = ongoingTaskSubscriptionListModel;

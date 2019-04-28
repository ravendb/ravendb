/// <reference path="../../../../typings/tsd.d.ts"/>
import appUrl = require("common/appUrl");
import router = require("plugins/router");
import generalUtils = require("common/generalUtils");
import ongoingTaskListModel = require("models/database/tasks/ongoingTaskListModel");
import ongoingTaskInfoCommand = require("commands/database/tasks/getOngoingTaskInfoCommand");
import subscriptionConnectionDetailsCommand = require("commands/database/tasks/getSubscriptionConnectionDetailsCommand");
import dropSubscriptionConnectionCommand = require("commands/database/tasks/dropSubscriptionConnectionCommand");
import getDocumentIDFromChangeVectorCommand = require("commands/database/documents/getDocumentIDFromChangeVectorCommand");
import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");
import changeVectorUtils = require("common/changeVectorUtils");

class ongoingTaskSubscriptionListModel extends ongoingTaskListModel {

    editUrl: KnockoutComputed<string>;
    activeDatabase = activeDatabaseTracker.default.database;

    // General stats
    lastTimeServerMadeProgressWithDocuments = ko.observable<string>();
    lastClientConnectionTime = ko.observable<string>();
    
    changeVectorForNextBatchStartingPoint = ko.observable<string>(null);
    changeVectorForNextBatchStartingPointFormatted: KnockoutComputed<changeVectorItem[]>;
    
    lastChangeVectorAcknowledged = ko.observable<string>(null);
    lastChangeVectorAcknowledgedFormatted: KnockoutComputed<changeVectorItem[]>;

    // Live connection stats
    clientIP = ko.observable<string>();
    connectionStrategy = ko.observable<Raven.Client.Documents.Subscriptions.SubscriptionOpeningStrategy>();  
    clientDetailsIssue = ko.observable<string>(); // null (ok) | client is not connected | failed to get details.. 
    textClass = ko.observable<string>("text-details");

    validationGroup: KnockoutValidationGroup; 
    showDetails = ko.observable(false);
   
    documentIDForNextBatchStartingPoint = ko.observable<string>(null);
    nextBatchStartingPointDocumentFound = ko.observable<boolean>(false);
    triedToFindDocumentNextBatchButFailed = ko.observable<boolean>(false);
    urlForNextBatchStartingPointDocument: KnockoutComputed<string>;

    documentIDForLastChangeVectorAcknowledged = ko.observable<string>(null);
    lastAcknowledgedDocumentFound  = ko.observable<boolean>(false);
    triedToFindDocumentLastAcknowledgedButFailed = ko.observable<boolean>(false);
    urlForLastAcknowledgedDocument: KnockoutComputed<string>;

    spinners = {
        documentIDForNextBatchStartingPointLoading: ko.observable<boolean>(false),
        documentIDForLastChangeVectorAcknowledgedLoading: ko.observable<boolean>(false)
    };
    
    constructor(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskSubscription) {
        super();

        this.update(dto);
        this.initializeObservables();
        this.bindToCurrentInstance("tryGetDocumentIDFromChangeVector");
    }

    protected bindToCurrentInstance(...methods: Array<keyof this>) {
        _.bindAll(this, ...methods);
    }

    initializeObservables() {
        super.initializeObservables();

        this.spinners.documentIDForNextBatchStartingPointLoading.extend({ rateLimit: 200});
        this.spinners.documentIDForLastChangeVectorAcknowledgedLoading.extend({ rateLimit: 200});

        const urls = appUrl.forCurrentDatabase();
        this.editUrl = urls.editSubscription(this.taskId, this.taskName());

        this.changeVectorForNextBatchStartingPointFormatted = ko.pureComputed(() => {
            const vector = this.changeVectorForNextBatchStartingPoint();
            return changeVectorUtils.formatChangeVector(vector, changeVectorUtils.shouldUseLongFormat([vector]));
        });
        
        this.lastChangeVectorAcknowledgedFormatted = ko.pureComputed(() => {
            const vector = this.lastChangeVectorAcknowledged();
            return changeVectorUtils.formatChangeVector(vector, changeVectorUtils.shouldUseLongFormat([vector]));
        });

        this.urlForNextBatchStartingPointDocument = ko.pureComputed(() => {
            return appUrl.forEditDoc(this.documentIDForNextBatchStartingPoint(), activeDatabaseTracker.default.database());
        });

        this.urlForLastAcknowledgedDocument = ko.pureComputed(() => {
            return appUrl.forEditDoc(this.documentIDForLastChangeVectorAcknowledged(), activeDatabaseTracker.default.database());
        });
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
                
                this.changeVectorForNextBatchStartingPoint(result.ChangeVectorForNextBatchStartingPoint);
                this.lastChangeVectorAcknowledged(result.LastChangeVectorAcknowledged);
                
                const dateFormat = generalUtils.dateFormat;

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
                    .fail((response: JQueryXHR) => {
                        if (response.status === 0) {
                            // we can't even connect to node, show node connectivity error
                            this.clientDetailsIssue("Failed to connect to " + this.responsibleNode().NodeUrl + ". Please make sure this url is accessible from your browser.");
                        } else {
                            this.clientDetailsIssue("Failed to get client connection details");    
                        }
                        
                        this.textClass("text-danger");
                    });
            })
            .always(() => {
                this.documentIDForNextBatchStartingPoint(null); 
                this.nextBatchStartingPointDocumentFound(false);
                this.triedToFindDocumentNextBatchButFailed(false);

                this.documentIDForLastChangeVectorAcknowledged(null);
                this.lastAcknowledgedDocumentFound(false);
                this.triedToFindDocumentLastAcknowledgedButFailed(false);
            });
    }

    disconnectClientFromSubscription() {
        new dropSubscriptionConnectionCommand(this.activeDatabase(), this.taskId, this.taskName())
            .execute()
            .done(() => { this.refreshSubscriptionInfo(); });
    }

    tryGetDocumentIDFromChangeVector(changeVectorType: string) {
        switch (changeVectorType) {
            case "NextBatchStartingPoint":
                this.spinners.documentIDForNextBatchStartingPointLoading(true);
                new getDocumentIDFromChangeVectorCommand(this.activeDatabase(), this.changeVectorForNextBatchStartingPoint())
                    .execute()
                    .done((result: Raven.Server.Documents.Handlers.DocumentIDDetails) => {
                        this.documentIDForNextBatchStartingPoint(result.DocId);
                        this.nextBatchStartingPointDocumentFound(true);
                    })
                    .fail(() => {
                        this.nextBatchStartingPointDocumentFound(false);
                        this.triedToFindDocumentNextBatchButFailed(true);
                    })
                    .always(() => this.spinners.documentIDForNextBatchStartingPointLoading(false));
                break;
            case "LastAcknowledgedByClient":
                this.spinners.documentIDForLastChangeVectorAcknowledgedLoading(true);
                new getDocumentIDFromChangeVectorCommand(this.activeDatabase(), this.lastChangeVectorAcknowledged())
                    .execute()
                    .done((result: Raven.Server.Documents.Handlers.DocumentIDDetails) => {
                        this.documentIDForLastChangeVectorAcknowledged(result.DocId);
                        this.lastAcknowledgedDocumentFound(true);
                    })
                    .fail(() => {
                        this.lastAcknowledgedDocumentFound(false);
                        this.triedToFindDocumentLastAcknowledgedButFailed(true);
                    })
                    .always(() => this.spinners.documentIDForLastChangeVectorAcknowledgedLoading(false));
                break;
            default:
                break;
        }
    }
}

export = ongoingTaskSubscriptionListModel;

/// <reference path="../../../../typings/tsd.d.ts"/>
import ongoingTaskSubscriptionModel = require("models/database/tasks/ongoingTaskSubscriptionModel");
import getRevisionsConfigurationCommand = require("commands/database/documents/getRevisionsConfigurationCommand");
import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");
import collectionsTracker = require("common/helpers/database/collectionsTracker");

class ongoingTaskSubscriptionEditModel extends ongoingTaskSubscriptionModel {

    liveConnection = ko.observable<boolean>();

    query = ko.observable<string>();

    startingPointType = ko.observable<subscriptionStartType>();
    startingChangeVector = ko.observable<string>();
    startingPointChangeVector: KnockoutComputed<boolean>;
    startingPointLatestDocument: KnockoutComputed<boolean>; 
    setStartingPoint = ko.observable<boolean>(true);
    
    changeVectorForNextBatchStartingPoint = ko.observable<string>(null); 

    validationGroup: KnockoutValidationGroup; 

    activeDatabase = activeDatabaseTracker.default.database;
   
    constructor(dto: Raven.Client.Documents.Subscriptions.SubscriptionStateWithNodeDetails, isInListView: boolean) {
        super(dto, isInListView);

        this.isInTasksListView = isInListView;
        this.query(dto.Query);
        this.editViewUpdate(dto);
        this.editViewInitializeObservables(); 
        this.editViewInitValidation();
    }

    editViewInitializeObservables() {
        super.listViewInitializeObservables();
        
        this.startingPointType("Beginning of Time");

        this.startingPointChangeVector = ko.pureComputed(() => {
            return this.startingPointType() === "Change Vector";
        });

        this.startingPointLatestDocument = ko.pureComputed(() => {
            return this.startingPointType() === "Latest Document";
        });
    }

    editViewUpdate(dto: Raven.Client.Documents.Subscriptions.SubscriptionStateWithNodeDetails) {
        this.query(dto.Query);
        this.changeVectorForNextBatchStartingPoint(dto.ChangeVectorForNextBatchStartingPoint);
        this.setStartingPoint(false);
    }

    dataFromUI(): subscriptionDataFromUI {
        const query = _.trim(this.query()) || null;
        
        let changeVector: Raven.Client.Constants.Documents.SubscriptionChangeVectorSpecialStates | string = "DoNotChange";

        if (this.setStartingPoint()) {
            switch (this.startingPointType()) {
            case "Beginning of Time":
                changeVector = "BeginningOfTime";
                break;
            case "Latest Document":
                changeVector = "LastDocument";
                break;
            case "Change Vector":
                changeVector = this.startingChangeVector();
                break;
            }
        }

        return {
            TaskName: this.taskName(),
            Query: query,
            ChangeVector: changeVector
        }
    }

    editViewInitValidation() {

        this.query.extend({
            required: true,
            aceValidation: true
        });

        this.startingChangeVector.extend({
            validation: [
                {
                    validator: () => {
                        const goodState1 = this.setStartingPoint() && this.startingPointType() === 'Change Vector' && this.startingChangeVector();
                        const goodState2 = this.setStartingPoint() && this.startingPointType() !== 'Change Vector';
                        const goodState3 = !this.setStartingPoint();
                        return goodState1 || goodState2 || goodState3;
                    },
                    message: "Please enter change vector"
                }]
        });

        this.validationGroup = ko.validatedObservable({
            query: this.query,
            startingChangeVector: this.startingChangeVector
        });
    }

    static empty(): ongoingTaskSubscriptionEditModel {
        return new ongoingTaskSubscriptionEditModel(
            {
                Disabled: false,
                Query: "",
                ChangeVectorForNextBatchStartingPoint: null,
                SubscriptionId: 0,
                SubscriptionName: null,
                ResponsibleNode: null,
                LastClientConnectionTime: null,
                LastTimeServerMadeProgressWithDocuments: null
            }, false);
    }
}

export = ongoingTaskSubscriptionEditModel;

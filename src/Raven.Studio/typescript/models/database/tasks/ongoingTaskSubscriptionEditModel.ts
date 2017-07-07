/// <reference path="../../../../typings/tsd.d.ts"/>
import ongoingTaskSubscriptionModel = require("models/database/tasks/ongoingTaskSubscriptionModel");

// This model is used by the 'Edit Subscription Task View'
class ongoingTaskSubscriptionEditModel extends ongoingTaskSubscriptionModel {

    script = ko.observable<string>();
    fromChangeVector = ko.observableArray<Raven.Client.Documents.Replication.Messages.ChangeVectorEntry>([]); 
    includeRevisions = ko.observable<boolean>(true);

    startingPointType = ko.observable<subscriptionStartType>();
    startingPointChangeVector: KnockoutComputed<boolean>;
    startingPointLatestDocument: KnockoutComputed<boolean>; 

    validationGroup: KnockoutValidationGroup; 
   
    constructor(dto: Raven.Client.Documents.Subscriptions.SubscriptionState) {
        super(dto);

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

    editViewUpdate(dto: Raven.Client.Documents.Subscriptions.SubscriptionState) {
        this.script(dto.Criteria.Script);
        this.fromChangeVector(dto.ChangeVector);
        this.includeRevisions(dto.Criteria.IsVersioned);
    }

    dataFromUI(): subscriptionDataFromUI {
        return {
            TaskName: this.taskName(),
            ChangeVectorEntry: null,
            // TODO:  Note: null means that we define with 'Beginning of Time'. This is temporary, until the other 2 options are implemented 
            Collection: this.collection(), 
            Script: this.script(),
            IsVersioned: this.includeRevisions()
        }
    }

    editViewInitValidation() {
        this.collection.extend({
            required: true
        });

        this.validationGroup = ko.validatedObservable({
            collection: this.collection
        });
    }

    static empty(): ongoingTaskSubscriptionEditModel {

        return new ongoingTaskSubscriptionEditModel(
            {
                Disabled: false,
                Criteria: {
                     Collection: null,
                     Script: null,
                     IsVersioned: false
                },
                ChangeVector: [],
                LastEtagReachedInServer: null,
                SubscriptionId: 0,
                SubscriptionName: null,
                TimeOfLastClientActivity: null
            });
    }
}

export = ongoingTaskSubscriptionEditModel;

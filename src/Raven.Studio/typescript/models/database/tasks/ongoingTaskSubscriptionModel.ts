/// <reference path="../../../../typings/tsd.d.ts"/>
import appUrl = require("common/appUrl");
import router = require("plugins/router");
import ongoingTask = require("models/database/tasks/ongoingTaskModel");

class ongoingTaskSubscriptionModel extends ongoingTask {

    editUrl: KnockoutComputed<string>;

    collection = ko.observable<string>();
    script = ko.observable<string>();
    timeOfLastClientActivity = ko.observable<string>(); 
    fromChangeVector = ko.observableArray<Raven.Client.Documents.Replication.Messages.ChangeVectorEntry>([]); 
    includeRevisions = ko.observable<boolean>(true);

    startingPointType = ko.observable<string>();
    startingPointChangeVector: KnockoutComputed<boolean>;
    startingPointLatestDocument: KnockoutComputed<boolean>; 

    validationGroup: KnockoutValidationGroup; 
    showSubscriptionDetails = ko.observable(false);

    // Note:
    // type OngoingTaskSubscription: for activating List View (Model is thinner, only needed values for List View)
    // type SubscriptionState:       for activating Edit View (Model is fatter..)
    constructor(dto: Raven.Server.Web.System.OngoingTaskSubscription | Raven.Client.Documents.Subscriptions.SubscriptionState ) {
        super();

        // Check the type passed:
        if ('Criteria' in dto) {

            // 1. Going to activate Edit View

            // 1.1 Create dto and call 'regular update'
            const dtoFatModel = dto as Raven.Client.Documents.Subscriptions.SubscriptionState;

            const state: Raven.Client.Server.Operations.OngoingTaskState = dtoFatModel.Disabled ? 'Disabled' : 'Enabled';
            const emptyNodeId: Raven.Client.Server.Operations.NodeId = { NodeTag: "", NodeUrl: "", ResponsibleNode: "" };

            const dtoThinModel: Raven.Server.Web.System.OngoingTaskSubscription = {
                Collection: dtoFatModel.Criteria.Collection,
                TimeOfLastClientActivity: dtoFatModel.TimeOfLastClientActivity,
                ResponsibleNode: emptyNodeId,
                TaskConnectionStatus: 'Active', // todo: this has to be reviewed...
                TaskId: dtoFatModel.SubscriptionId,
                TaskName: dtoFatModel.SubscriptionName,
                TaskState: state,
                TaskType: 'Subscription'
            };
            this.update(dtoThinModel);
          
            // 1.2 call 'edit view update' for the rest...
            this.editViewUpdate(dtoFatModel);

        } else {
            // 2. Going to activate List View
            this.update(dto as Raven.Server.Web.System.OngoingTaskSubscription);
        }

        this.initializeObservables(); 
        this.initValidation();
    }

    initializeObservables() {
        super.initializeObservables();

        const urls = appUrl.forCurrentDatabase();
        this.editUrl = urls.editSubscription(this.taskId, this.taskName());

        this.startingPointType("Beginning of Time");

        this.startingPointChangeVector = ko.pureComputed(() => {
            if (this.startingPointType() === 'Beginning of Time' || this.startingPointType() === 'Latest Document') {
                return false;
            }
            return true;
        });

        this.startingPointLatestDocument = ko.pureComputed(() => {
            if (this.startingPointType() === 'Beginning of Time' || this.startingPointType() === 'Change Vector') {
                return false;
            }
            return true;
        });
    }

    editViewUpdate(dtoFatModel: Raven.Client.Documents.Subscriptions.SubscriptionState) {
        this.script(dtoFatModel.Criteria.Script);
        this.fromChangeVector(dtoFatModel.ChangeVector);
        this.includeRevisions(dtoFatModel.Criteria.IsVersioned);
    }

    update(dto: Raven.Server.Web.System.OngoingTaskSubscription) {
        super.update(dto);
        this.collection(dto.Collection);
        this.timeOfLastClientActivity(dto.TimeOfLastClientActivity);
    }

    editTask() {
        router.navigate(this.editUrl());
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

    initValidation() {
        this.collection.extend({
            required: true
        });

        this.validationGroup = ko.validatedObservable({
            collection: this.collection
        });
    }

    static empty(): ongoingTaskSubscriptionModel {

        return new ongoingTaskSubscriptionModel({
            TaskId: null,
            TaskName: "",
            TaskType: "Subscription"
        } as Raven.Server.Web.System.OngoingTaskSubscription);
    }
}

export = ongoingTaskSubscriptionModel;

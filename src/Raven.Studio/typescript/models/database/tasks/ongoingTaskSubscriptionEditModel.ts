/// <reference path="../../../../typings/tsd.d.ts"/>
import ongoingTaskEditModel = require("models/database/tasks/ongoingTaskEditModel");
import jsonUtil = require("common/jsonUtil");

class ongoingTaskSubscriptionEditModel extends ongoingTaskEditModel {

    liveConnection = ko.observable<boolean>();

    query = ko.observable<string>();

    startingPointType = ko.observable<subscriptionStartType>();
    startingChangeVector = ko.observable<string>();
    startingPointChangeVector: KnockoutComputed<boolean>;
    startingPointLatestDocument: KnockoutComputed<boolean>; 
    setStartingPoint = ko.observable<boolean>(true);
    
    changeVectorForNextBatchStartingPoint = ko.observable<string>(null); 

    validationGroup: KnockoutValidationGroup; 
    
    dirtyFlag: () => DirtyFlag;

    constructor(dto: Raven.Client.Documents.Subscriptions.SubscriptionStateWithNodeDetails) {
        super();
        
        this.query(dto.Query.trim());
        this.updateDetails(dto);
        this.initializeObservables(); 
        this.initValidation();
        
        this.dirtyFlag = new ko.DirtyFlag([
            this.taskName,
            this.mentorNode,
            this.manualChooseMentor,
            this.query,
            this.startingPointType,
            this.startingChangeVector, 
            this.setStartingPoint,
            this.changeVectorForNextBatchStartingPoint
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }

    initializeObservables() {
        super.initializeObservables();
        
        this.startingPointType("Beginning of Time");

        this.startingPointChangeVector = ko.pureComputed(() => {
            return this.startingPointType() === "Change Vector";
        });

        this.startingPointLatestDocument = ko.pureComputed(() => {
            return this.startingPointType() === "Latest Document";
        });
    }   

    updateDetails(dto: Raven.Client.Documents.Subscriptions.SubscriptionStateWithNodeDetails) {
        const dtoEditModel = dto as Raven.Client.Documents.Subscriptions.SubscriptionStateWithNodeDetails;

        const state: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskState = dtoEditModel.Disabled ? 'Disabled' : 'Enabled';
        const emptyNodeId: Raven.Client.ServerWide.Operations.NodeId = { NodeTag: "", NodeUrl: "", ResponsibleNode: "" };

        const dtoListModel: Raven.Client.Documents.Operations.OngoingTasks.OngoingTask = {
            ResponsibleNode: emptyNodeId,
            TaskConnectionStatus: 'Active',
            TaskId: dtoEditModel.SubscriptionId,
            TaskName: dtoEditModel.SubscriptionName,
            MentorNode: dtoEditModel.MentorNode,
            TaskState: state,
            TaskType: 'Subscription',
            Error: null
        };

        super.update(dtoListModel);
        
        this.manualChooseMentor(!!dto.MentorNode);

        this.query(dto.Query.trim());
        this.changeVectorForNextBatchStartingPoint(dto.ChangeVectorForNextBatchStartingPoint);
        this.setStartingPoint(false);
    }

    private serializeChangeVector() {
        let changeVector: Raven.Client.Constants.Documents.SubscriptionChangeVectorSpecialStates | string = this.taskId ? "DoNotChange" : "BeginningOfTime";

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
        return changeVector;
    }
    
    toTestDto() {
        const subscriptionToTest: Raven.Client.Documents.Subscriptions.SubscriptionTryout = {
            ChangeVector: this.serializeChangeVector(),
            Query: this.query()
        };
    }
    
    toDto(): Raven.Client.Documents.Subscriptions.SubscriptionCreationOptions {
        const query = _.trim(this.query()) || null;

        return {
            Name: this.taskName(),
            Query: query,
            MentorNode: this.manualChooseMentor() ? this.mentorNode() : undefined,
            ChangeVector: this.serializeChangeVector()
        }
    }

    initValidation() {
        this.query.extend({
            required: true,
            aceValidation: true
        });
        
        this.initializeMentorValidation();

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
            startingChangeVector: this.startingChangeVector,
            mentorNode: this.mentorNode
            
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
                LastBatchAckTime: null,
                MentorNode: null,
                NodeTag: null
            });
    }
}

export = ongoingTaskSubscriptionEditModel;

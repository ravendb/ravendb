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

    get studioTaskType(): StudioTaskType {
        return "Subscription";
    }
    
    constructor(dto: Raven.Client.Documents.Subscriptions.SubscriptionStateWithNodeDetails) {
        super();
        
        this.query(dto.Query);
        this.updateDetails(dto);
        this.initializeObservables(); 
        this.initValidation();
        
        this.dirtyFlag = new ko.DirtyFlag([
            this.taskName,
            this.taskState,
            this.mentorNode,
            this.manualChooseMentor,
            this.pinMentorNode,
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
            PinToMentorNode: dtoEditModel.PinToMentorNode,
            TaskState: state,
            TaskType: 'Subscription',
            Error: null
        };

        super.update(dtoListModel);
        
        this.manualChooseMentor(!!dto.MentorNode);
        this.pinMentorNode(dto.PinToMentorNode);

        this.query(dto.Query);
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
                    changeVector = this.startingChangeVector().trim().replace(/\r?\n/g, " ");
                    break;
            }
        }
        return changeVector;
    }
    
    toDto(): Raven.Client.Documents.Subscriptions.SubscriptionCreationOptions {
        return {
            Name: this.taskName(),
            Query: this.query() || null,
            MentorNode: this.manualChooseMentor() ? this.mentorNode() : undefined,
            PinToMentorNode: this.pinMentorNode(),
            ChangeVector: this.serializeChangeVector(),
            Disabled: this.taskState() === "Disabled"
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
                NodeTag: null,
                ChangeVectorForNextBatchStartingPointPerShard: null,
                PinToMentorNode: false,
            });
    }
}

export = ongoingTaskSubscriptionEditModel;

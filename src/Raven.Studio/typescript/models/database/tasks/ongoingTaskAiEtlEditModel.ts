/// <reference path="../../../../typings/tsd.d.ts"/>
import ongoingTaskEditModel = require("models/database/tasks/ongoingTaskEditModel");
import ongoingTaskElasticSearchEtlTransformationModel = require("models/database/tasks/ongoingTaskElasticSearchEtlTransformationModel");

class ongoingTaskAiEtlEditModel extends ongoingTaskEditModel {
    connectionStringName = ko.observable<string>();

    allowEtlOnNonEncryptedChannel = ko.observable<boolean>(false);
    
    transformationScripts = ko.observableArray<ongoingTaskElasticSearchEtlTransformationModel>([]);
    
    validationGroup: KnockoutValidationGroup;
    enterTestModeValidationGroup: KnockoutValidationGroup;
    dirtyFlag: () => DirtyFlag;

    get studioTaskType(): StudioTaskType {
        return "ElasticSearchEtl";
    }

    get destinationType(): TaskDestinationType {
        return "Index";
    }
    
    constructor(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskAiEtl) {
        super();

        this.update(dto);
        this.initializeObservables();
        this.initializeValidation();
    }
    
    protected initializeObservables() {
        super.initializeObservables();
        
        this.dirtyFlag = new ko.DirtyFlag([ 
            this.taskName,
            this.taskState,
            this.connectionStringName,
            this.mentorNode,
            this.pinMentorNode,
            this.manualChooseMentor,
            this.allowEtlOnNonEncryptedChannel
        ])
    }
    
    initializeValidation() {
        this.initializeMentorValidation();

        this.connectionStringName.extend({
            required: true
        });

       
        
        this.transformationScripts.extend({
            validation: [
                {
                    validator: () => this.transformationScripts().length > 0,
                    message: "Transformation Script is Not defined"
                }
            ]
        });

        this.validationGroup = ko.validatedObservable({
            connectionStringName: this.connectionStringName,
            transformationScripts: this.transformationScripts,
            mentorNode: this.mentorNode
        });
    }

    update(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskAiEtl) {
        super.update(dto);
        
        const configuration = dto.Configuration;
        if (configuration) {
            this.connectionStringName(configuration.ConnectionStringName);
            this.allowEtlOnNonEncryptedChannel(configuration.AllowEtlOnNonEncryptedChannel);
            this.manualChooseMentor(!!configuration.MentorNode);
            this.pinMentorNode(configuration.PinToMentorNode);
            this.mentorNode(configuration.MentorNode);
            
            if (configuration.Transforms) {
                this.transformationScripts(configuration.Transforms.map(x => new ongoingTaskElasticSearchEtlTransformationModel(x, false, false)));
            }
        }
    }
    
    toDto(): Raven.Client.Documents.Operations.ETL.AI.AiEtlConfiguration {
        return {
            TaskId: this.taskId,
            Name: this.taskName(),
            EtlType: "Ai",
            ConnectionStringName: this.connectionStringName(),
            AllowEtlOnNonEncryptedChannel: this.allowEtlOnNonEncryptedChannel(),
            Disabled: this.taskState() === "Disabled",
            MentorNode: this.manualChooseMentor() ? this.mentorNode() : undefined,
            PinToMentorNode: this.pinMentorNode(),
            Transforms: this.transformationScripts().map(x => x.toDto()),
            AiConnectorType: "AzureOpenAI", // TODO kalczur
            FieldsToInclude: []
        };
        
    }
    
    static empty(): ongoingTaskAiEtlEditModel {
        return new ongoingTaskAiEtlEditModel(
            {
                TaskName: "",
                TaskType: "AiEtl",
                TaskState: "Enabled",
                TaskConnectionStatus: "Active",
                Configuration: {
                    Transforms: [],
                }
            } as Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskAiEtl);
       }
}

export = ongoingTaskAiEtlEditModel;

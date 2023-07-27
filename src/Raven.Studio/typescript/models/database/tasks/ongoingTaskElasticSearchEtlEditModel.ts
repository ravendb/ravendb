/// <reference path="../../../../typings/tsd.d.ts"/>
import ongoingTaskEditModel = require("models/database/tasks/ongoingTaskEditModel");
import ongoingTaskElasticSearchEtlTransformationModel = require("models/database/tasks/ongoingTaskElasticSearchEtlTransformationModel");
import ongoingTaskElasticSearchEtlIndexModel = require("models/database/tasks/ongoingTaskElasticSearchEtlIndexModel");

class ongoingTaskElasticSearchEtlEditModel extends ongoingTaskEditModel {
    connectionStringName = ko.observable<string>();
        
    transformationScripts = ko.observableArray<ongoingTaskElasticSearchEtlTransformationModel>([]);
    elasticIndexes = ko.observableArray<ongoingTaskElasticSearchEtlIndexModel>([]);
    
    validationGroup: KnockoutValidationGroup;
    enterTestModeValidationGroup: KnockoutValidationGroup;
    dirtyFlag: () => DirtyFlag;

    get studioTaskType(): StudioTaskType {
        return "ElasticSearchEtl";
    }

    get destinationType(): TaskDestinationType {
        return "Index";
    }
    
    constructor(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskElasticSearchEtlDetails) {
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
            this.manualChooseMentor
        ])
    }
    
    initializeValidation() {
        this.initializeMentorValidation();

        this.connectionStringName.extend({
            required: true
        });

        this.elasticIndexes.extend({
            validation: [
                {
                    validator: () => this.elasticIndexes().length > 0,
                    message: "Elasticsearch index is Not defined"
                }
            ]
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
            elasticIndexes: this.elasticIndexes,
            transformationScripts: this.transformationScripts,
            mentorNode: this.mentorNode
        });
        
        this.enterTestModeValidationGroup = ko.validatedObservable({
            elasticIndexes: this.elasticIndexes
        });
    }

    update(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskElasticSearchEtlDetails) {
        super.update(dto);
        
        const configuration = dto.Configuration;
        if (configuration) {
            this.connectionStringName(configuration.ConnectionStringName);
            this.manualChooseMentor(!!configuration.MentorNode);
            this.pinMentorNode(configuration.PinToMentorNode);
            this.mentorNode(configuration.MentorNode);
            
            if (configuration.Transforms) {
                this.transformationScripts(configuration.Transforms.map(x => new ongoingTaskElasticSearchEtlTransformationModel(x, false, false)));
            }
            
            if (configuration.ElasticIndexes) {
                this.elasticIndexes(configuration.ElasticIndexes.map(x => new ongoingTaskElasticSearchEtlIndexModel(x, false)));
            }
        }
    }
    
    toDto(): Raven.Client.Documents.Operations.ETL.ElasticSearch.ElasticSearchEtlConfiguration {
        return {
            TaskId: this.taskId,
            Name: this.taskName(),
            EtlType: "ElasticSearch",
            ConnectionStringName: this.connectionStringName(),
            AllowEtlOnNonEncryptedChannel: true,
            Disabled: this.taskState() === "Disabled",
            MentorNode: this.manualChooseMentor() ? this.mentorNode() : undefined,
            PinToMentorNode: this.pinMentorNode(),
            Transforms: this.transformationScripts().map(x => x.toDto()),
            ElasticIndexes: this.elasticIndexes().map(x => x.toDto())
        };
        
    }
    
    static empty(): ongoingTaskElasticSearchEtlEditModel {
        return new ongoingTaskElasticSearchEtlEditModel(
            {
                TaskName: "",
                TaskType: "ElasticSearchEtl",
                TaskState: "Enabled",
                TaskConnectionStatus: "Active",
                Configuration: {
                    Transforms: [],
                    ElasticIndexes: []
                }
            } as Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskElasticSearchEtlDetails);
       }
}

export = ongoingTaskElasticSearchEtlEditModel;

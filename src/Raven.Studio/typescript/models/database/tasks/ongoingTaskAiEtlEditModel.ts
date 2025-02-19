/// <reference path="../../../../typings/tsd.d.ts"/>
import ongoingTaskEditModel = require("models/database/tasks/ongoingTaskEditModel");
import ongoingTaskAiTransformationModel = require("models/database/tasks/ongoingTaskAiTransformationModel");
import TaskUtils = require("components/utils/TaskUtils");

class ongoingTaskAiEtlEditModel extends ongoingTaskEditModel {
    identifier = ko.observable<string>();
    connectionStringName = ko.observable<string>();

    allowEtlOnNonEncryptedChannel = ko.observable<boolean>(false);
    
    transformationScripts = ko.observableArray<ongoingTaskAiTransformationModel>([]);
    
    validationGroup: KnockoutValidationGroup;
    enterTestModeValidationGroup: KnockoutValidationGroup;
    dirtyFlag: () => DirtyFlag;

    get studioTaskType(): StudioTaskType {
        return "AiIntegration";
    }

    get destinationType(): TaskDestinationType {
        return "Index";
    }
    
    constructor(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskAiIntegration) {
        super();

        this.update(dto);
        this.initializeObservables();
        this.initializeValidation();
    }
    
    protected initializeObservables() {
        super.initializeObservables();
        
        this.dirtyFlag = new ko.DirtyFlag([ 
            this.taskName,
            this.identifier,
            this.taskState,
            this.connectionStringName,
            this.mentorNode,
            this.pinMentorNode,
            this.manualChooseMentor,
            this.allowEtlOnNonEncryptedChannel
        ]);
    }
    
    initializeValidation() {
        this.initializeMentorValidation();

        this.taskName.extend({
            required: true
        });

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

    generateIdentifierOnNameBlur() {
        if (!this.identifier()) {
            this.generateIdentifier();
        }
    }

    generateIdentifier() {
        this.identifier(TaskUtils.default.getGeneratedIdentifier(this.taskName()));
    }

    update(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskAiIntegration) {
        super.update(dto);
        
        const configuration = dto.Configuration;
        if (configuration) {
            this.connectionStringName(configuration.ConnectionStringName);
            this.allowEtlOnNonEncryptedChannel(configuration.AllowEtlOnNonEncryptedChannel);
            this.manualChooseMentor(!!configuration.MentorNode);
            this.pinMentorNode(configuration.PinToMentorNode);
            this.mentorNode(configuration.MentorNode);
            this.identifier(configuration.Identifier);

            if (configuration.Transforms) {
                this.transformationScripts(configuration.Transforms.map(x => new ongoingTaskAiTransformationModel(x, false, true, configuration.EmbeddingsPaths?.length ? "paths" : "script", configuration.EmbeddingsPaths ?? [])));
            }
        }
    }
    
    toDto(): AiIntegrationConfiguration {
        // only one transformation is supported
        const transformation = this.transformationScripts()[0];

        const EmbeddingsTransformation: Raven.Client.Documents.Operations.AI.AiEmbeddingsTransformation = transformation.embeddingsSource() === "script" ? {
            Script: transformation.script(),
        } : null;

        const EmbeddingsPaths: string[] = transformation.embeddingsSource() === "paths" ? transformation.embeddingsPaths() : [];

        return {
            TaskId: this.taskId,
            Name: this.taskName(),
            Identifier: this.identifier(),
            EtlType: "Ai",
            ConnectionStringName: this.connectionStringName(),
            AllowEtlOnNonEncryptedChannel: this.allowEtlOnNonEncryptedChannel(),
            Disabled: this.taskState() === "Disabled",
            MentorNode: this.manualChooseMentor() ? this.mentorNode() : undefined,
            PinToMentorNode: this.pinMentorNode(),
            Transforms: [transformation.toDto()],
            Collection: transformation.transformScriptCollections()[0],
            EmbeddingsPaths,
            EmbeddingsTransformation
        };
    }
    
    static empty(): ongoingTaskAiEtlEditModel {
        return new ongoingTaskAiEtlEditModel(
            {
                TaskName: "",
                TaskType: "AiIntegration",
                TaskState: "Enabled",
                TaskConnectionStatus: "Active",
                Configuration: {
                    Transforms: [],
                    Identifier: ""
                }
            } as Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskAiIntegration);
       }
}

export = ongoingTaskAiEtlEditModel;

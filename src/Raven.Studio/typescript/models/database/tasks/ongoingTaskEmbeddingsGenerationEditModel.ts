/// <reference path="../../../../typings/tsd.d.ts"/>
import ongoingTaskEditModel = require("models/database/tasks/ongoingTaskEditModel");
import ongoingTaskEmbeddingsGenerationTransformationModel = require("models/database/tasks/ongoingTaskEmbeddingsGenerationTransformationModel");
import TaskUtils = require("components/utils/TaskUtils");
import TimeInSeconds = require("common/constants/timeInSeconds");

class ongoingTaskEmbeddingsGenerationEditModel extends ongoingTaskEditModel {
    identifier = ko.observable<string>();
    connectionStringName = ko.observable<string>();

    allowEtlOnNonEncryptedChannel = ko.observable<boolean>(false);
    
    transformationScripts = ko.observableArray<ongoingTaskEmbeddingsGenerationTransformationModel>([]);
    
    validationGroup: KnockoutValidationGroup;
    enterTestModeValidationGroup: KnockoutValidationGroup;
    dirtyFlag: () => DirtyFlag;

    maxTokensPerChunk = ko.observable<number>();
    maxTokensPerChunkDefaultValue: KnockoutObservable<number>;

    chunkingMethod = ko.observable<Raven.Client.Documents.Operations.AI.ChunkingMethod>("PlainTextSplitLines");
    chunkingMethodOptions: valueAndLabelItem<Raven.Client.Documents.Operations.AI.ChunkingMethod, string>[] = [
        { value: "PlainTextSplitLines", label: "Plain Text Split Lines" },
        { value: "PlainTextSplitParagraphs", label: "Plain Text Split Paragraphs" },
        { value: "MarkDownSplitLines", label: "Markdown Split Lines" },
        { value: "MarkDownSplitParagraphs", label: "Markdown Split Paragraphs" },
        { value: "HtmlSplitLines", label: "HTML Split Lines" },
        { value: "HtmlStrip", label: "HTML Strip" },
    ];
    chunkingMethodLabel: KnockoutComputed<string>;

    quantizationType = ko.observable<Raven.Client.Documents.Indexes.Vector.VectorEmbeddingType>("Single");
    quantizationTypeOptions: valueAndLabelItem<Raven.Client.Documents.Indexes.Vector.VectorEmbeddingType, string>[] = [
        { value: "Single", label: "Single (no quantization)" },
        { value: "Int8", label: "Int8" },
        { value: "Binary", label: "Binary" }
    ];
    quantizationTypeLabel: KnockoutComputed<string>;

    embeddingsCacheExpiration = ko.observable<number>(TimeInSeconds.TimeInSeconds.Day * 14);

    aiConnectionStrings: KnockoutObservableArray<Raven.Client.Documents.Operations.AI.AiConnectionString>;


    get studioTaskType(): StudioTaskType {
        return "AiIntegration";
    }

    get destinationType(): TaskDestinationType {
        return "Index";
    }
    
    constructor(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskAiIntegration, aiConnectionStrings: KnockoutObservableArray<Raven.Client.Documents.Operations.AI.AiConnectionString>) {
        super();

        this.aiConnectionStrings = aiConnectionStrings;

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
            this.allowEtlOnNonEncryptedChannel,
            this.chunkingMethod,
            this.maxTokensPerChunk,
            this.quantizationType,
            this.embeddingsCacheExpiration,
        ]);

        this.maxTokensPerChunkDefaultValue = ko.pureComputed(() => {
            const connectionString = this.aiConnectionStrings().find(x => x.Name === this.connectionStringName());
            
            if (connectionString?.OnnxSettings) {
                return 512;
            }
            return 2048;
        });

        this.quantizationTypeLabel = ko.pureComputed(() => {
            return this.quantizationTypeOptions.find(x => x.value === this.quantizationType())?.label || "Select quantization type";
        });

        this.chunkingMethodLabel = ko.pureComputed(() => {
            return this.chunkingMethodOptions.find(x => x.value === this.chunkingMethod())?.label || "Select chunking method";
        });
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
            taskName: this.taskName,
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
            if (configuration.ChunkingOptionsForQuerying) {
                this.chunkingMethod(configuration.ChunkingOptionsForQuerying.ChunkingMethod);
                this.maxTokensPerChunk(configuration.ChunkingOptionsForQuerying.MaxTokensPerChunk);
            }
            if (configuration.TargetQuantizationType) {
                this.quantizationType(configuration.TargetQuantizationType);
            }

            // TODO add expiration

            if (configuration.Transforms) {
                this.transformationScripts(configuration.Transforms.map(x => new ongoingTaskEmbeddingsGenerationTransformationModel(
                    x,
                    false,
                    true,
                    configuration.EmbeddingsPathConfigurations?.length ? "paths" : "script",
                    configuration.EmbeddingsPathConfigurations ?? [],
                    this.maxTokensPerChunkDefaultValue
                )));
            }
        }
    }
    
    toDto(): Raven.Client.Documents.Operations.AI.EmbeddingsGenerationConfiguration {
        // only one transformation is supported
        const transformation = this.transformationScripts()[0];

        const EmbeddingsTransformation: Raven.Client.Documents.Operations.AI.EmbeddingsTransformation = transformation.embeddingsSource() === "script" ? {
            Script: transformation.script(),
        } : null;

        const EmbeddingsPathConfigurations = transformation.embeddingsSource() === "paths" ? transformation.embeddingPathConfigurations() : [];

        // TODO add expiration

        return {
            TaskId: this.taskId,
            Name: this.taskName(),
            Identifier: this.identifier(),
            EtlType: "EmbeddingsGeneration",
            ConnectionStringName: this.connectionStringName(),
            AllowEtlOnNonEncryptedChannel: this.allowEtlOnNonEncryptedChannel(),
            Disabled: this.taskState() === "Disabled",
            MentorNode: this.manualChooseMentor() ? this.mentorNode() : undefined,
            PinToMentorNode: this.pinMentorNode(),
            Transforms: [transformation.toDto()],
            Collection: transformation.transformScriptCollections()[0],
            ChunkingOptionsForQuerying: {
                ChunkingMethod: this.chunkingMethod(),
                MaxTokensPerChunk: this.maxTokensPerChunk(),
            },
            TargetQuantizationType: this.quantizationType(),
            EmbeddingsTransformation,
            EmbeddingsPathConfigurations
        };
    }
    
    static empty(aiConnectionStrings: KnockoutObservableArray<Raven.Client.Documents.Operations.AI.AiConnectionString>): ongoingTaskEmbeddingsGenerationEditModel {
        return new ongoingTaskEmbeddingsGenerationEditModel(
            {
                TaskName: "",
                TaskType: "AiIntegration",
                TaskState: "Enabled",
                TaskConnectionStatus: "Active",
                Configuration: {
                    Transforms: [],
                    Identifier: ""
                }
            } as Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskAiIntegration,
            aiConnectionStrings
        );
       }
}

export = ongoingTaskEmbeddingsGenerationEditModel;

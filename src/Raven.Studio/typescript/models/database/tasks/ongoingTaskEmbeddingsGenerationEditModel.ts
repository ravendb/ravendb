/// <reference path="../../../../typings/tsd.d.ts"/>
import ongoingTaskEditModel = require("models/database/tasks/ongoingTaskEditModel");
import TaskUtils = require("components/utils/TaskUtils");
import TimeInSeconds = require("common/constants/timeInSeconds");
import genUtils = require("common/generalUtils");

type EmbeddingsSource = "script" | "paths";

class ongoingTaskEmbeddingsGenerationEditModel extends ongoingTaskEditModel {
    identifier = ko.observable<string>();
    connectionStringName = ko.observable<string>();

    allowEtlOnNonEncryptedChannel = ko.observable<boolean>(false);
    
    validationGroup: KnockoutValidationGroup;
    enterTestModeValidationGroup: KnockoutValidationGroup;
    dirtyFlag: () => DirtyFlag;

    maxTokensPerChunk = ko.observable<number>();
    maxTokensPerChunkDefaultValue: KnockoutObservable<number>;

    // path configuration inputs
    pathConfigurationMaxTokensPerChunk = ko.observable<number>();
    pathConfigurationChunkingMethod = ko.observable<Raven.Client.Documents.Operations.AI.ChunkingMethod>("PlainTextSplitLines");
    pathConfigurationChunkingMethodLabel: KnockoutComputed<string>;
    pathConfigurationPath = ko.observable<string>("");

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

    embeddingsCacheExpiration = ko.observable<number>(TimeInSeconds.TimeInSeconds.Day * 90);
    embeddingsCacheForQueryingExpiration = ko.observable<number>(TimeInSeconds.TimeInSeconds.Day * 14);

    embeddingsSource = ko.observable<EmbeddingsSource>("paths");
    embeddingsSourceLabel: KnockoutComputed<string>;

    script = ko.observable<string>();

    embeddingPathConfigurations = ko.observableArray<Raven.Client.Documents.Operations.AI.EmbeddingPathConfiguration>([]);

    collectionInput = ko.observable<string>();

    aiConnectionStrings: KnockoutObservableArray<Raven.Client.Documents.Operations.AI.AiConnectionString>;

    isNew = ko.observable<boolean>(true);
    resetScript = ko.observable<boolean>(false);
    isResetAlreadySet = ko.observable<boolean>(false);

    transforms = ko.observableArray<Raven.Client.Documents.Operations.ETL.Transformation>([]);

    get studioTaskType(): StudioTaskType {
        return "EmbeddingsGeneration";
    }

    get destinationType(): TaskDestinationType {
        return "Index";
    }
    
    constructor(dto: Raven.Client.Documents.Operations.OngoingTasks.EmbeddingsGeneration, aiConnectionStrings: KnockoutObservableArray<Raven.Client.Documents.Operations.AI.AiConnectionString>) {
        super();

        this.aiConnectionStrings = aiConnectionStrings;

        this.update(dto);
        this.initializeObservables();
        this.initializeValidation();
    }
    
    protected initializeObservables() {
        super.initializeObservables();
        
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

        this.pathConfigurationChunkingMethodLabel = ko.pureComputed(() => {
            return this.chunkingMethodOptions.find(x => x.value === this.pathConfigurationChunkingMethod())?.label || "Select chunking method";
        });

        this.embeddingsSourceLabel = ko.pureComputed(() => {
            const source = this.embeddingsSource();

            if (source === "script") {
                return "Script";
            }
            if (source === "paths") {
                return "Paths";
            }
            return genUtils.assertUnreachable(source);
        });

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
            this.embeddingsCacheForQueryingExpiration,
            this.embeddingsSource,
            this.script,
            this.embeddingPathConfigurations,
            this.resetScript,
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

        this.script.extend({
            required: {
                onlyIf: () => this.embeddingsSource() === "script"
            },
            aceValidation: true
        });

        this.embeddingPathConfigurations.extend({
            validation: [
                {
                    onlyIf: () => this.embeddingsSource() === "paths",
                    validator: () => this.embeddingPathConfigurations().length > 0,
                    message: "At least one path is required"
                }
            ]
        });

        this.collectionInput.extend({
            required: true
        });

        this.maxTokensPerChunk.extend({
            min: 1,
        });

        this.validationGroup = ko.validatedObservable({
            taskName: this.taskName,
            connectionStringName: this.connectionStringName,
            mentorNode: this.mentorNode,
            script: this.script,
            embeddingPathConfigurations: this.embeddingPathConfigurations,
            collectionInput: this.collectionInput,
            maxTokensPerChunk: this.maxTokensPerChunk
        });
    }

    setResetScriptIfEdit() {
        if (!this.isNew() && !this.isResetAlreadySet()) {
            this.resetScript(true);
            this.isResetAlreadySet(true);
        }
    }

    generateIdentifierOnNameBlur() {
        if (!this.identifier()) {
            this.generateIdentifier();
        }
    }

    generateIdentifier() {
        this.identifier(TaskUtils.default.getGeneratedIdentifier(this.taskName()));
    }

    addEmbeddingsPathConfiguration(): void {
        this.embeddingPathConfigurations.push({
            Path: this.pathConfigurationPath(),
            ChunkingOptions: {
                ChunkingMethod: this.pathConfigurationChunkingMethod(),
                MaxTokensPerChunk: this.pathConfigurationMaxTokensPerChunk() ?? this.maxTokensPerChunkDefaultValue()
            }
        });
        this.pathConfigurationPath("");
        this.pathConfigurationMaxTokensPerChunk(null);
        this.pathConfigurationChunkingMethod("PlainTextSplitLines");
    }

    removeEmbeddingsPathConfiguration(path: string): void {
        this.embeddingPathConfigurations.remove(x => x.Path === path);
    }

    update(dto: Raven.Client.Documents.Operations.OngoingTasks.EmbeddingsGeneration) {
        super.update(dto);

        const configuration = dto.Configuration;
        if (configuration) {
            this.isNew(false);

            this.connectionStringName(configuration.ConnectionStringName);
            this.allowEtlOnNonEncryptedChannel(configuration.AllowEtlOnNonEncryptedChannel);
            this.manualChooseMentor(!!configuration.MentorNode);
            this.pinMentorNode(configuration.PinToMentorNode);
            this.mentorNode(configuration.MentorNode);
            this.identifier(configuration.Identifier);

            if (configuration.Transforms) {
                this.transforms(configuration.Transforms);
            }
            if (configuration.Collection) {
                this.collectionInput(configuration.Collection);
            }
            if (configuration.ChunkingOptionsForQuerying) {
                this.chunkingMethod(configuration.ChunkingOptionsForQuerying.ChunkingMethod);
                this.maxTokensPerChunk(configuration.ChunkingOptionsForQuerying.MaxTokensPerChunk);
            }
            if (configuration.Quantization) {
                this.quantizationType(configuration.Quantization);
            }
            if (configuration.EmbeddingsCacheExpiration) {
                this.embeddingsCacheExpiration(genUtils.timeSpanToSeconds(configuration.EmbeddingsCacheExpiration));
            }
            if (configuration.EmbeddingsCacheForQueryingExpiration) {
                this.embeddingsCacheForQueryingExpiration(genUtils.timeSpanToSeconds(configuration.EmbeddingsCacheForQueryingExpiration));
            }
            if (configuration.EmbeddingsPathConfigurations) {
                this.embeddingPathConfigurations(configuration.EmbeddingsPathConfigurations);
            }
            if (configuration.EmbeddingsTransformation?.Script) {
                this.script(configuration.EmbeddingsTransformation.Script);
                this.embeddingsSource("script");
            } else {
                this.embeddingsSource("paths");
            }
        }
    }
    
    toDto(): Raven.Client.Documents.Operations.AI.EmbeddingsGenerationConfiguration {
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
            Transforms: null,
            Collection: this.collectionInput(),
            ChunkingOptionsForQuerying: {
                ChunkingMethod: this.chunkingMethod(),
                MaxTokensPerChunk: this.maxTokensPerChunk() ?? this.maxTokensPerChunkDefaultValue(),
            },
            Quantization: this.quantizationType(),
            EmbeddingsTransformation: this.embeddingsSource() === "script" ? { Script: this.script() } : null,
            EmbeddingsPathConfigurations: this.embeddingsSource() === "paths" ? this.embeddingPathConfigurations() : [],
            EmbeddingsCacheExpiration: genUtils.formatAsTimeSpan(this.embeddingsCacheExpiration() * 1000),
            EmbeddingsCacheForQueryingExpiration: genUtils.formatAsTimeSpan(this.embeddingsCacheForQueryingExpiration() * 1000)
        };
    }
    
    static empty(aiConnectionStrings: KnockoutObservableArray<Raven.Client.Documents.Operations.AI.AiConnectionString>): ongoingTaskEmbeddingsGenerationEditModel {
        return new ongoingTaskEmbeddingsGenerationEditModel(
            {
                TaskName: "",
                TaskType: "EmbeddingsGeneration",
                TaskState: "Enabled",
                TaskConnectionStatus: "Active",
            } as Raven.Client.Documents.Operations.OngoingTasks.EmbeddingsGeneration,
            aiConnectionStrings
        );
       }
}

export = ongoingTaskEmbeddingsGenerationEditModel;

/// <reference path="../../../../typings/tsd.d.ts"/>
import ongoingTaskEditModel = require("models/database/tasks/ongoingTaskEditModel");
import TaskUtils = require("components/utils/TaskUtils");
import TimeInSeconds = require("common/constants/timeInSeconds");
import genUtils = require("common/generalUtils");

type EmbeddingsSource = "script" | "paths";

const defaultChunkingMethod: Raven.Client.Documents.Operations.AI.ChunkingMethod = "PlainTextSplit";
const defaultEmbeddingsCacheForQueryingExpiration = TimeInSeconds.TimeInSeconds.Day * 14;

class ongoingTaskEmbeddingsGenerationEditModel extends ongoingTaskEditModel {
    identifier = ko.observable<string>();
    connectionStringName = ko.observable<string>();

    allowEtlOnNonEncryptedChannel = ko.observable<boolean>(false);
    
    validationGroup: KnockoutValidationGroup;
    addPathConfigurationValidationGroup: KnockoutValidationGroup;
    enterTestModeValidationGroup: KnockoutValidationGroup;
    dirtyFlag: () => DirtyFlag;

    maxTokensPerChunkDefaultValue: KnockoutObservable<number>;

    chunkingMethodOptions: valueAndLabelItem<Raven.Client.Documents.Operations.AI.ChunkingMethod, string>[] = [
        { value: "PlainTextSplit", label: "Plain Text: Split" },
        { value: "PlainTextSplitLines", label: "Plain Text: Split Lines" },
        { value: "PlainTextSplitParagraphs", label: "Plain Text: Split Paragraphs" },
        { value: "MarkDownSplitLines", label: "Markdown: Split Lines" },
        { value: "MarkDownSplitParagraphs", label: "Markdown: Split Paragraphs" },
        { value: "HtmlStrip", label: "HTML: Strip" },
    ];

    // path configuration inputs
    pathConfigurationChunkingMethod = ko.observable<Raven.Client.Documents.Operations.AI.ChunkingMethod>(defaultChunkingMethod);
    pathConfigurationChunkingMethodLabel: KnockoutComputed<string>;
    pathConfigurationPath = ko.observable<string>("");
    pathConfigurationMaxTokensPerChunk = ko.observable<number>(null);
    pathConfigurationOverlapTokens = ko.observable<number>(null);

    // transformation inputs
    transformationChunkingMethod = ko.observable<Raven.Client.Documents.Operations.AI.ChunkingMethod>(defaultChunkingMethod);
    transformationChunkingMethodLabel: KnockoutComputed<string>;
    transformationMaxTokensPerChunk = ko.observable<number>(null);
    transformationOverlapTokens = ko.observable<number>(null);

    // querying inputs
    queryingChunkingMethod = ko.observable<Raven.Client.Documents.Operations.AI.ChunkingMethod>(defaultChunkingMethod);
    queryingChunkingMethodLabel: KnockoutComputed<string>;
    queryingMaxTokensPerChunk = ko.observable<number>(null);
    queryingOverlapTokens = ko.observable<number>(null);
    embeddingsCacheForQueryingExpiration = ko.observable<number>(defaultEmbeddingsCacheForQueryingExpiration);

    quantizationType = ko.observable<Raven.Client.Documents.Indexes.Vector.VectorEmbeddingType>("Single");
    quantizationTypeOptions: valueAndLabelItem<Raven.Client.Documents.Indexes.Vector.VectorEmbeddingType, string>[] = [
        { value: "Single", label: "Single (no quantization)" },
        { value: "Int8", label: "Int8" },
        { value: "Binary", label: "Binary" }
    ];
    quantizationTypeLabel: KnockoutComputed<string>;

    embeddingsCacheExpiration = ko.observable<number>(TimeInSeconds.TimeInSeconds.Day * 90);

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

    isQueryingOpen = ko.observable<boolean>(false);

    canUseOverlapTokens: KnockoutComputed<boolean>;
    canUseOverlapTokensForPathConfiguration: KnockoutComputed<boolean>;
    canUseOverlapTokensForTransformation: KnockoutComputed<boolean>;

    get studioTaskType(): StudioTaskType {
        return "EmbeddingsGeneration";
    }

    get destinationType(): TaskDestinationType {
        return "Index";
    }
    
    constructor(dto: Raven.Client.Documents.Operations.OngoingTasks.EmbeddingsGeneration, aiConnectionStrings: KnockoutObservableArray<Raven.Client.Documents.Operations.AI.AiConnectionString>) {
        super();

        this.aiConnectionStrings = aiConnectionStrings;

        this.initializeObservables();
        this.update(dto);
        this.initializeValidation();
        this.initializeDirtyFlag();
    }

    private resetOverlapTokensIfNotSupported = (chunkingMethod: Raven.Client.Documents.Operations.AI.ChunkingMethod, type: "pathConfiguration" | "transformation" | "querying") => {
        if (chunkingMethod !== "PlainTextSplitParagraphs" && chunkingMethod !== "MarkDownSplitParagraphs") {
            switch (type) {
                case "transformation":
                    this.transformationOverlapTokens(null);
                    break;
                case "pathConfiguration":
                    this.pathConfigurationOverlapTokens(null);
                    break;
                case "querying":
                    this.queryingOverlapTokens(null);
                    break;
                default:
                    genUtils.assertUnreachable(type);
            }
        }
    }
    
    protected initializeObservables() {
        super.initializeObservables();

        this.pathConfigurationChunkingMethod.subscribe((newValue) => this.resetOverlapTokensIfNotSupported(newValue, "pathConfiguration"));
        this.transformationChunkingMethod.subscribe((newValue) => this.resetOverlapTokensIfNotSupported(newValue, "transformation"));
        this.queryingChunkingMethod.subscribe(newValue => this.resetOverlapTokensIfNotSupported(newValue, "querying"));

        this.maxTokensPerChunkDefaultValue = ko.pureComputed(() => {
            const connectionString = this.aiConnectionStrings().find(x => x.Name === this.connectionStringName());
            
            if (connectionString?.EmbeddedSettings) {
                return 512;
            }
            return 2048;
        });

        this.quantizationTypeLabel = ko.pureComputed(() => {
            return this.quantizationTypeOptions.find(x => x.value === this.quantizationType())?.label || "Select quantization type";
        });

        this.queryingChunkingMethodLabel = ko.pureComputed(() => {
            return this.chunkingMethodOptions.find(x => x.value === this.queryingChunkingMethod())?.label || "Select chunking method";
        });

        this.transformationChunkingMethodLabel = ko.pureComputed(() => {
            return this.chunkingMethodOptions.find(x => x.value === this.transformationChunkingMethod())?.label || "Select chunking method";
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


        this.canUseOverlapTokens = ko.pureComputed(() => {
            return this.queryingChunkingMethod() === "PlainTextSplitParagraphs" || this.queryingChunkingMethod() === "MarkDownSplitParagraphs";
        })

        this.canUseOverlapTokensForPathConfiguration = ko.pureComputed(() => {
            return this.pathConfigurationChunkingMethod() === "PlainTextSplitParagraphs" || this.pathConfigurationChunkingMethod() === "MarkDownSplitParagraphs";
        })

        this.canUseOverlapTokensForTransformation = ko.pureComputed(() => {
            return this.transformationChunkingMethod() === "PlainTextSplitParagraphs" || this.transformationChunkingMethod() === "MarkDownSplitParagraphs";
        })
    }

    initializeDirtyFlag() {
        this.dirtyFlag = new ko.DirtyFlag([ 
            this.taskName,
            this.identifier,
            this.taskState,
            this.connectionStringName,
            this.collectionInput,
            this.mentorNode,
            this.pinMentorNode,
            this.manualChooseMentor,
            this.allowEtlOnNonEncryptedChannel,
            this.queryingChunkingMethod,
            this.queryingMaxTokensPerChunk,
            this.queryingOverlapTokens,
            this.transformationOverlapTokens,
            this.quantizationType,
            this.embeddingsCacheExpiration,
            this.embeddingsCacheForQueryingExpiration,
            this.embeddingsSource,
            this.script,
            this.embeddingPathConfigurations,
            this.resetScript,
            this.pathConfigurationMaxTokensPerChunk,
            this.pathConfigurationOverlapTokens,
            this.pathConfigurationPath,
        ]);
    }
    
    initializeValidation() {
        this.initializeMentorValidation();

        this.taskName.extend({
            required: true
        });

        this.identifier.extend({
            validation: [
                {
                    validator: () => /^[a-z0-9-]+$/.test(this.identifier()),
                    message: "Only lowercase letters (a-z), numbers (0-9) and hyphens (-) are allowed."
                }
            ]
        });

        this.connectionStringName.extend({
            required: true
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

        this.queryingMaxTokensPerChunk.extend({
            min: 1,
        });

        this.queryingOverlapTokens.extend({
            min: 0,
            validation: [
                {
                    onlyIf: () => this.queryingChunkingMethod() === "PlainTextSplitParagraphs" || this.queryingChunkingMethod() === "MarkDownSplitParagraphs",
                    validator: () => {
                        const overlapTokens = this.queryingOverlapTokens() ?? 0;
                        const maxTokensPerChunk = this.queryingMaxTokensPerChunk() ?? this.maxTokensPerChunkDefaultValue();
                        return overlapTokens <= maxTokensPerChunk;
                    },
                    message: "Overlap tokens must be less than or equal to max tokens per chunk"
                }
            ]
        });

        this.script.extend({
            required: {
                onlyIf: () => this.embeddingsSource() === "script"
            },
            aceValidation: true
        });

        this.transformationMaxTokensPerChunk.extend({
            min: {
                params: 1,
                onlyIf: () => this.embeddingsSource() === "script",
            },
        });

        this.transformationOverlapTokens.extend({
            min: {
                params: 0,
                onlyIf: () => this.embeddingsSource() === "script" && (this.transformationChunkingMethod() === "PlainTextSplitParagraphs" || this.transformationChunkingMethod() === "MarkDownSplitParagraphs"),
            },
            validation: [
                {
                    onlyIf: () => this.embeddingsSource() === "script" && (this.transformationChunkingMethod() === "PlainTextSplitParagraphs" || this.transformationChunkingMethod() === "MarkDownSplitParagraphs"),
                    validator: () => {
                        const overlapTokens = this.transformationOverlapTokens() ?? 0;
                        const maxTokensPerChunk = this.transformationMaxTokensPerChunk() ?? this.maxTokensPerChunkDefaultValue();
                        return overlapTokens <= maxTokensPerChunk;
                    },
                    message: "Overlap tokens must be less than or equal to max tokens per chunk"
                }
            ]
        });
        
        this.embeddingsCacheExpiration.extend({
            min: 1,
        });

        this.embeddingsCacheForQueryingExpiration.extend({
            min: 1,
        });

        this.validationGroup = ko.validatedObservable({
            taskName: this.taskName,
            identifier: this.identifier,
            connectionStringName: this.connectionStringName,
            mentorNode: this.mentorNode,
            script: this.script,
            embeddingPathConfigurations: this.embeddingPathConfigurations,
            collectionInput: this.collectionInput,
            queryingMaxTokensPerChunk: this.queryingMaxTokensPerChunk,
            queryingOverlapTokens: this.queryingOverlapTokens,
            transformationMaxTokensPerChunk: this.transformationMaxTokensPerChunk,
            transformationOverlapTokens: this.transformationOverlapTokens,
            embeddingsCacheExpiration: this.embeddingsCacheExpiration,
            embeddingsCacheForQueryingExpiration: this.embeddingsCacheForQueryingExpiration,
        });

        this.pathConfigurationPath.extend({
            required: {
                onlyIf: () => this.embeddingsSource() === "paths"
            },
            validation: [
                {
                    onlyIf: () => this.embeddingsSource() === "paths",
                    validator: () => !this.embeddingPathConfigurations().map(x => x.Path).includes(this.pathConfigurationPath()),
                    message: "Path already exists"
                }
            ]
        });

        this.pathConfigurationMaxTokensPerChunk.extend({
            min: {
                params: 1,
                onlyIf: () => this.embeddingsSource() === "paths",
            },
        });

        this.pathConfigurationOverlapTokens.extend({
            min: {
                params: 0,
                onlyIf: () => this.embeddingsSource() === "paths" && (this.pathConfigurationChunkingMethod() === "PlainTextSplitParagraphs" || this.pathConfigurationChunkingMethod() === "MarkDownSplitParagraphs")
            },
            validation: [
                {
                    onlyIf: () => this.embeddingsSource() === "paths" && (this.pathConfigurationChunkingMethod() === "PlainTextSplitParagraphs" || this.pathConfigurationChunkingMethod() === "MarkDownSplitParagraphs"),
                    validator: () => {
                        const overlapTokens = this.pathConfigurationOverlapTokens() ?? 0;
                        const maxTokensPerChunk = this.pathConfigurationMaxTokensPerChunk() ?? this.maxTokensPerChunkDefaultValue();
                        return overlapTokens <= maxTokensPerChunk;
                    },
                    message: "Overlap tokens must be less than or equal to max tokens per chunk"
                }
            ]
        });

        this.addPathConfigurationValidationGroup = ko.validatedObservable({
            pathConfigurationPath: this.pathConfigurationPath,
            pathConfigurationMaxTokensPerChunk: this.pathConfigurationMaxTokensPerChunk,
            pathConfigurationOverlapTokens: this.pathConfigurationOverlapTokens,
        });
    }

    toggleQueryingAccordion() {
        this.isQueryingOpen(!this.isQueryingOpen());
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
        if (!this.addPathConfigurationValidationGroup.isValid()) {
            this.addPathConfigurationValidationGroup.errors.showAllMessages(true);
            return;
        }

        this.embeddingPathConfigurations.push({
            Path: this.pathConfigurationPath(),
            ChunkingOptions: {
                OverlapTokens: this.pathConfigurationOverlapTokens() ?? 0,
                ChunkingMethod: this.pathConfigurationChunkingMethod(),
                MaxTokensPerChunk: this.pathConfigurationMaxTokensPerChunk() ?? this.maxTokensPerChunkDefaultValue(),
                ContextPrefix: null
            }
        });
        this.pathConfigurationPath("");
        this.pathConfigurationMaxTokensPerChunk(null);
        this.pathConfigurationChunkingMethod("PlainTextSplitLines");
        this.pathConfigurationOverlapTokens(null);

        this.addPathConfigurationValidationGroup.errors.showAllMessages(false);
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
                this.queryingChunkingMethod(configuration.ChunkingOptionsForQuerying.ChunkingMethod);
                this.queryingMaxTokensPerChunk(configuration.ChunkingOptionsForQuerying.MaxTokensPerChunk);
                this.queryingOverlapTokens(configuration.ChunkingOptionsForQuerying.OverlapTokens);
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

            if (configuration.EmbeddingsTransformation?.ChunkingOptions) {
                this.transformationChunkingMethod(configuration.EmbeddingsTransformation.ChunkingOptions.ChunkingMethod);
                this.transformationMaxTokensPerChunk(configuration.EmbeddingsTransformation.ChunkingOptions.MaxTokensPerChunk);
                this.transformationOverlapTokens(configuration.EmbeddingsTransformation.ChunkingOptions.OverlapTokens);
            }

            // Open the querying section if some value is different from the default
            if (
                configuration.EmbeddingsCacheForQueryingExpiration !== genUtils.formatAsTimeSpan(defaultEmbeddingsCacheForQueryingExpiration * 1000) ||
                (configuration.ChunkingOptionsForQuerying && (
                    configuration.ChunkingOptionsForQuerying.MaxTokensPerChunk !== this.maxTokensPerChunkDefaultValue() ||
                    configuration.ChunkingOptionsForQuerying.ChunkingMethod !== defaultChunkingMethod ||
                    (configuration.ChunkingOptionsForQuerying.OverlapTokens !== null && configuration.ChunkingOptionsForQuerying.OverlapTokens !== 0)
                ))
            ) {
                this.isQueryingOpen(true);
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
                OverlapTokens: this.queryingOverlapTokens() ?? 0,
                ChunkingMethod: this.queryingChunkingMethod(),
                MaxTokensPerChunk: this.queryingMaxTokensPerChunk() ?? this.maxTokensPerChunkDefaultValue(),
                ContextPrefix: null
            },
            Quantization: this.quantizationType(),
            EmbeddingsTransformation: this.embeddingsSource() === "script" ? {
                Script: this.script(), ChunkingOptions: { OverlapTokens: this.transformationOverlapTokens() ?? 0, MaxTokensPerChunk: this.transformationMaxTokensPerChunk() ?? this.maxTokensPerChunkDefaultValue(), ChunkingMethod: this.transformationChunkingMethod(), ContextPrefix: null }
            } : null,
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

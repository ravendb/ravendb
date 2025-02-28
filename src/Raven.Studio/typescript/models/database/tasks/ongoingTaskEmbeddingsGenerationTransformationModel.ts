/// <reference path="../../../../typings/tsd.d.ts"/>
import genUtils = require("common/generalUtils");
import collectionsTracker = require("common/helpers/database/collectionsTracker");
import jsonUtil = require("common/jsonUtil");
import TimeInSeconds = require("common/constants/timeInSeconds");

type EmbeddingsSource = "script" | "paths";

class ongoingTaskEmbeddingsGenerationTransformationModel {
    name = ko.observable<string>("not-empty");
    script = ko.observable<string>();

    static readonly applyToAllCollectionsText = "Apply to All Collections";
    
    isNew = ko.observable<boolean>(true);
    resetScript = ko.observable<boolean>(false);

    inputCollection = ko.observable<string>();

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
    
    embeddingsSource = ko.observable<EmbeddingsSource>("script");
    embeddingsSourceLabel: KnockoutComputed<string>;
    
    inputEmbeddingsPath = ko.observable<string>("");
    embeddingsPaths = ko.observableArray<string>([]);

    embeddingPathConfigurations = ko.observableArray<Raven.Client.Documents.Operations.AI.EmbeddingPathConfiguration>([]);

    transformScriptCollections = ko.observableArray<string>([]);
    
    canAddCollection: KnockoutComputed<boolean>;
    applyScriptForAllCollections = ko.observable<boolean>(false);

    embeddingsCacheExpiration = ko.observable<number>(TimeInSeconds.TimeInSeconds.Day * 90);

    validationGroup: KnockoutValidationGroup;

    dirtyFlag: () => DirtyFlag;

    constructor(
        dto: Raven.Client.Documents.Operations.ETL.Transformation,
        isNew: boolean, resetScript: boolean,
        embeddingsSource: EmbeddingsSource,
        embeddingPathConfigurations: Raven.Client.Documents.Operations.AI.EmbeddingPathConfiguration[],
        maxTokensPerChunkDefaultValue: KnockoutObservable<number>
    ) {
        
        this.update(dto, isNew, resetScript, embeddingsSource, embeddingPathConfigurations);
        this.maxTokensPerChunkDefaultValue = maxTokensPerChunkDefaultValue;

        this.initObservables();
        this.initValidation();
    }

    static isApplyToAll(collectionName: string){
        return collectionName === ongoingTaskEmbeddingsGenerationTransformationModel.applyToAllCollectionsText;
    }

    initObservables(): void {
        this.canAddCollection = ko.pureComputed(() => {
            const collectionToAdd = this.inputCollection();
            return collectionToAdd && !this.transformScriptCollections().find(x => x === collectionToAdd);
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

        this.chunkingMethodLabel = ko.pureComputed(() => {
            return this.chunkingMethodOptions.find(x => x.value === this.chunkingMethod())?.label || "Select chunking method";
        });
        
        this.dirtyFlag = new ko.DirtyFlag([
            this.name,
            this.script,
            this.resetScript,
            this.applyScriptForAllCollections,
            this.transformScriptCollections,
            this.embeddingsSource,
            this.embeddingsCacheExpiration,
            this.embeddingPathConfigurations
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }

    static empty(maxTokensPerChunkDefaultValue: KnockoutObservable<number>, name?: string): ongoingTaskEmbeddingsGenerationTransformationModel {
        return new ongoingTaskEmbeddingsGenerationTransformationModel(
            {
                ApplyToAllDocuments: false,
                Collections: [],
                Disabled: false,
                Name: name || "",
                Script: "",
                DocumentIdPostfix: null
            }, true, false, "script", [], maxTokensPerChunkDefaultValue);
    }

    toDto(): Raven.Client.Documents.Operations.ETL.Transformation {
        return {
            ApplyToAllDocuments: this.applyScriptForAllCollections(),
            Collections: this.applyScriptForAllCollections() ? null : this.transformScriptCollections(),
            Disabled: false,
            Name: this.name(),
            Script: this.script(),
            DocumentIdPostfix: null
        }
    }

    private initValidation(): void {

        this.script.extend({
            required: {
                onlyIf: () => this.embeddingsSource() === "script"
            },
            aceValidation: true
        });

        this.transformScriptCollections.extend({
            validation: [
                {
                    validator: () => this.transformScriptCollections().length > 0,
                    message: "At least one collection is required"
                }
            ]
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

        this.validationGroup = ko.validatedObservable({
            script: this.script,
            transformScriptCollections: this.transformScriptCollections,
            embeddingPathConfigurations: this.embeddingPathConfigurations
        });
    }

    addCollection(): void {
        this.addWithBlink(this.inputCollection());
    }
    
    removeCollection(collection: string): void {
        this.transformScriptCollections.remove(collection);
        this.applyScriptForAllCollections(false);
    }

    addEmbeddingsPathConfiguration(): void {
        this.embeddingPathConfigurations.push({
            Path: this.inputEmbeddingsPath(),
            ChunkingOptions: {
                ChunkingMethod: this.chunkingMethod(),
                MaxTokensPerChunk: this.maxTokensPerChunk() ?? this.maxTokensPerChunkDefaultValue()
            }
        });
        this.inputEmbeddingsPath("");
        this.maxTokensPerChunk(null);
        this.chunkingMethod("PlainTextSplitLines");
    }

    removeEmbeddingsPathConfiguration(path: string): void {
        this.embeddingPathConfigurations.remove(x => x.Path === path);
    }

    addWithBlink(collectionName: string): void {
        if (ongoingTaskEmbeddingsGenerationTransformationModel.isApplyToAll(collectionName)) {
            this.applyScriptForAllCollections(true);
            this.transformScriptCollections([ongoingTaskEmbeddingsGenerationTransformationModel.applyToAllCollectionsText]);
        } else {
            this.applyScriptForAllCollections(false);
            this.transformScriptCollections.unshift(collectionName);
            this.transformScriptCollections.remove(ongoingTaskEmbeddingsGenerationTransformationModel.applyToAllCollectionsText);
        }

        this.inputCollection("");

        // blink on newly created item
        $(".collection-list li").first().addClass("blink-style");
    }

    update(dto: Raven.Client.Documents.Operations.ETL.Transformation, isNew: boolean, resetScript: boolean, embeddingsSource: EmbeddingsSource, embeddingPathConfigurations: Raven.Client.Documents.Operations.AI.EmbeddingPathConfiguration[]): void {
        this.name(dto.Name);
        this.script(dto.Script);
        this.embeddingsSource(embeddingsSource ?? "script");
        this.embeddingPathConfigurations(embeddingPathConfigurations ?? []);
        
        this.transformScriptCollections(dto.Collections || []);
        this.applyScriptForAllCollections(dto.ApplyToAllDocuments);

        if (this.applyScriptForAllCollections()) {
            this.transformScriptCollections([ongoingTaskEmbeddingsGenerationTransformationModel.applyToAllCollectionsText]);
        }
        
        this.isNew(isNew);
        this.resetScript(resetScript);
    }

    getCollectionEntry(collectionName: string): number {
        return collectionsTracker.default.getCollectionColorIndex(collectionName);
    }

    hasUpdates(oldItem: this): boolean {
        const hashFunction = jsonUtil.newLineNormalizingHashFunctionWithIgnoredFields(["__moduleId__", "validationGroup"]);
        return hashFunction(this) !== hashFunction(oldItem);
    }
}

export = ongoingTaskEmbeddingsGenerationTransformationModel;

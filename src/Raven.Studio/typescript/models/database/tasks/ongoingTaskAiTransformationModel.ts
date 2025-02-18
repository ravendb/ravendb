/// <reference path="../../../../typings/tsd.d.ts"/>
import genUtils = require("common/generalUtils");
import collectionsTracker = require("common/helpers/database/collectionsTracker");
import jsonUtil = require("common/jsonUtil");

type EmbeddingsSource = "script" | "paths";

class ongoingTaskAiTransformationModel {
    name = ko.observable<string>("not-empty");
    script = ko.observable<string>();

    static readonly applyToAllCollectionsText = "Apply to All Collections";
    
    isNew = ko.observable<boolean>(true);
    resetScript = ko.observable<boolean>(false);

    inputCollection = ko.observable<string>();
    
    embeddingsSource = ko.observable<EmbeddingsSource>("script");
    embeddingsSourceLabel: KnockoutComputed<string>;
    
    inputEmbeddingsPath = ko.observable<string>("");
    embeddingsPaths = ko.observableArray<string>([]);
    transformScriptCollections = ko.observableArray<string>([]);
    
    canAddCollection: KnockoutComputed<boolean>;
    applyScriptForAllCollections = ko.observable<boolean>(false);

    validationGroup: KnockoutValidationGroup;

    dirtyFlag: () => DirtyFlag;

    constructor(dto: Raven.Client.Documents.Operations.ETL.Transformation, isNew: boolean, resetScript: boolean, embeddingsSource: EmbeddingsSource, embeddingsPaths: string[]) {
        this.update(dto, isNew, resetScript, embeddingsSource, embeddingsPaths);

        this.initObservables();
        this.initValidation();
    }

    static isApplyToAll(collectionName: string){
        return collectionName === ongoingTaskAiTransformationModel.applyToAllCollectionsText;
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
        
        this.dirtyFlag = new ko.DirtyFlag([
            this.name,
            this.script,
            this.resetScript,
            this.applyScriptForAllCollections,
            this.transformScriptCollections,
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }

    static empty(name?: string): ongoingTaskAiTransformationModel {
        return new ongoingTaskAiTransformationModel(
            {
                ApplyToAllDocuments: false,
                Collections: [],
                Disabled: false,
                Name: name || "",
                Script: "",
                DocumentIdPostfix: null
            }, true, false, "script", []);
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

        this.embeddingsPaths.extend({
            validation: [
                {
                    onlyIf: () => this.embeddingsSource() === "paths",
                    validator: () => this.embeddingsPaths().length > 0,
                    message: "At least one path is required"
                }
            ]
        });

        this.validationGroup = ko.validatedObservable({
            script: this.script,
            transformScriptCollections: this.transformScriptCollections,
            embeddingsPaths: this.embeddingsPaths
        });
    }

    addCollection(): void {
        this.addWithBlink(this.inputCollection());
    }
    
    removeCollection(collection: string): void {
        this.transformScriptCollections.remove(collection);
        this.applyScriptForAllCollections(false);
    }

    addEmbeddingsPath(): void {
        this.embeddingsPaths.push(this.inputEmbeddingsPath());
        this.inputEmbeddingsPath("");
    }

    removeEmbeddingsPath(path: string): void {
        this.embeddingsPaths.remove(path);
    }

    addWithBlink(collectionName: string): void {
        if (ongoingTaskAiTransformationModel.isApplyToAll(collectionName)) {
            this.applyScriptForAllCollections(true);
            this.transformScriptCollections([ongoingTaskAiTransformationModel.applyToAllCollectionsText]);
        } else {
            this.applyScriptForAllCollections(false);
            this.transformScriptCollections.unshift(collectionName);
            this.transformScriptCollections.remove(ongoingTaskAiTransformationModel.applyToAllCollectionsText);
        }

        this.inputCollection("");

        // blink on newly created item
        $(".collection-list li").first().addClass("blink-style");
    }

    update(dto: Raven.Client.Documents.Operations.ETL.Transformation, isNew: boolean, resetScript: boolean, embeddingsSource: EmbeddingsSource, embeddingsPaths: string[]): void {
        this.name(dto.Name);
        this.script(dto.Script);
        this.embeddingsSource(embeddingsSource ?? "script");
        this.embeddingsPaths(embeddingsPaths ?? []);
        
        this.transformScriptCollections(dto.Collections || []);
        this.applyScriptForAllCollections(dto.ApplyToAllDocuments);

        if (this.applyScriptForAllCollections()) {
            this.transformScriptCollections([ongoingTaskAiTransformationModel.applyToAllCollectionsText]);
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

export = ongoingTaskAiTransformationModel;

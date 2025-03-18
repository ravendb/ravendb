/// <reference path="../../../../typings/tsd.d.ts"/>
import collectionsTracker = require("common/helpers/database/collectionsTracker");
import jsonUtil = require("common/jsonUtil");

class ongoingTaskSnowflakeEtlTransformationModel {
    name = ko.observable<string>();
    script = ko.observable<string>();
    
    static readonly applyToAllCollectionsText = "Apply to All Collections";
    
    isNew = ko.observable<boolean>(true);
    resetScript = ko.observable<boolean>(false);

    inputCollection = ko.observable<string>();
    transformScriptCollections = ko.observableArray<string>([]);

    canAddCollection: KnockoutComputed<boolean>;
    applyScriptForAllCollections = ko.observable<boolean>(false);

    validationGroup: KnockoutValidationGroup; 
  
    dirtyFlag: () => DirtyFlag;
    
    constructor(dto: Raven.Client.Documents.Operations.ETL.Transformation, isNew: boolean, resetScript: boolean) {
        this.update(dto, isNew, resetScript);
        
        this.initObservables();
        this.initValidation();
    }

    static isApplyToAll(colectionName: string){
        return colectionName === ongoingTaskSnowflakeEtlTransformationModel.applyToAllCollectionsText;
    }
    
    initObservables(): void {
        this.canAddCollection = ko.pureComputed(() => {
            const collectionToAdd = this.inputCollection();
            return collectionToAdd && !this.transformScriptCollections().find(x => x === collectionToAdd);
        });
        
        this.dirtyFlag = new ko.DirtyFlag([
            this.name, 
            this.script,
            this.resetScript,
            this.applyScriptForAllCollections,
            this.transformScriptCollections,
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }
   
    static empty(name?: string): ongoingTaskSnowflakeEtlTransformationModel {
        return new ongoingTaskSnowflakeEtlTransformationModel(
            {
                ApplyToAllDocuments: false, 
                Collections: [],
                Disabled: false,
                Name: name || "",
                Script: "",
                DocumentIdPostfix: null
            }, true, false);
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
        this.name.extend({
            required: true
        })
        
        this.script.extend({
            required: true,
            aceValidation: true
        });
        
        this.transformScriptCollections.extend({
            validation: [
                {
                    validator: () => this.applyScriptForAllCollections() || this.transformScriptCollections().length > 0,
                    message: "At least one collection is required"
                }
            ]
        });

        this.validationGroup = ko.validatedObservable({
            name: this.name,
            script: this.script,
            transformScriptCollections: this.transformScriptCollections
        });
    }

    addCollection(): void {
        this.addWithBlink(this.inputCollection());
    }

    removeCollection(collection: string): void {
        this.transformScriptCollections.remove(collection);
        this.applyScriptForAllCollections(false);
    }

    addWithBlink(collectionName: string): void {
        if (ongoingTaskSnowflakeEtlTransformationModel.isApplyToAll(collectionName)) {
            this.applyScriptForAllCollections(true);
            this.transformScriptCollections([ongoingTaskSnowflakeEtlTransformationModel.applyToAllCollectionsText]);
        } else {
            this.applyScriptForAllCollections(false);
            this.transformScriptCollections.unshift(collectionName);
            this.transformScriptCollections.remove(ongoingTaskSnowflakeEtlTransformationModel.applyToAllCollectionsText);
        }

        this.inputCollection("");

        // blink on newly created item
        $(".collection-list li").first().addClass("blink-style");
    }

    update(dto: Raven.Client.Documents.Operations.ETL.Transformation, isNew: boolean, resetScript: boolean): void {
        this.name(dto.Name);
        this.script(dto.Script);
        
        this.transformScriptCollections(dto.Collections || []);
        this.applyScriptForAllCollections(dto.ApplyToAllDocuments);

        if (this.applyScriptForAllCollections()) {
            this.transformScriptCollections([ongoingTaskSnowflakeEtlTransformationModel.applyToAllCollectionsText]);
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

export = ongoingTaskSnowflakeEtlTransformationModel;

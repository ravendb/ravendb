/// <reference path="../../../../typings/tsd.d.ts"/>
import collectionsTracker = require("common/helpers/database/collectionsTracker");
import jsonUtil = require("common/jsonUtil");

class ongoingTaskRavenEtlTransformationModel {
    
    static readonly applyToAllCollectionsText = "Apply to All Collections";
    
    name = ko.observable<string>();
    script = ko.observable<string>();
    transformScriptCollections = ko.observableArray<string>([]);

    resetScript = ko.observable<boolean>(false);
    applyScriptForAllCollections = ko.observable<boolean>(false);
    isNew = ko.observable<boolean>(true);
    inputCollection = ko.observable<string>();
    canAddCollection: KnockoutComputed<boolean>;

    validationGroup: KnockoutValidationGroup; 
    
    dirtyFlag: () => DirtyFlag;
  
    constructor(dto: Raven.Client.Documents.Operations.ETL.Transformation, isNew: boolean, resetScript: boolean) {
        this.update(dto, isNew, resetScript);
        this.initObservables();
        this.initValidation();

        this.dirtyFlag = new ko.DirtyFlag([
            this.name,
            this.script,
            this.resetScript,
            this.applyScriptForAllCollections,
            this.transformScriptCollections], 
        false, jsonUtil.newLineNormalizingHashFunction);
    }

    static isApplyToAll(colectionName: string){
        return colectionName === ongoingTaskRavenEtlTransformationModel.applyToAllCollectionsText;
    }
    
    getCollectionEntry(collectionName: string) {
        return collectionsTracker.default.getCollectionColorIndex(collectionName);
    }

    static empty(): ongoingTaskRavenEtlTransformationModel {
        return new ongoingTaskRavenEtlTransformationModel(
            {
                ApplyToAllDocuments: false, 
                Collections: [],
                Disabled: false,
                Name: "",
                Script: ""
            }, true, false);
    }

    toDto(): Raven.Client.Documents.Operations.ETL.Transformation {
        return {
            ApplyToAllDocuments: this.applyScriptForAllCollections(),
            Collections: this.applyScriptForAllCollections() ? null : this.transformScriptCollections(),
            Disabled: false,
            Name: this.name(),
            Script: this.script()
        }
    }

    private initObservables() {
        this.canAddCollection = ko.pureComputed(() => {
            const collectionToAdd = this.inputCollection();
            return collectionToAdd && !this.transformScriptCollections().find(x => x === collectionToAdd);
        });
    }

    private initValidation() {
        this.transformScriptCollections.extend({
            validation: [
                {
                    validator: () => this.applyScriptForAllCollections() || this.transformScriptCollections().length > 0,
                    message: "At least one collection is required"
                }
            ]
        });
        
        this.script.extend({
            aceValidation: true
        });

        this.validationGroup = ko.validatedObservable({
            transformScriptCollections: this.transformScriptCollections,
            script: this.script
        });
    }

    removeCollection(collection: string) {
        this.transformScriptCollections.remove(collection);
        this.applyScriptForAllCollections(false);
    }

    addCollection() {
        this.addWithBlink(this.inputCollection());
    }
    
    addWithBlink(collectionName: string) {
        if (ongoingTaskRavenEtlTransformationModel.isApplyToAll(collectionName)) {
            this.applyScriptForAllCollections(true);
            this.transformScriptCollections([ongoingTaskRavenEtlTransformationModel.applyToAllCollectionsText]);
        } else {
            this.applyScriptForAllCollections(false);
            _.remove(this.transformScriptCollections(), x => x === ongoingTaskRavenEtlTransformationModel.applyToAllCollectionsText);
            this.transformScriptCollections.unshift(collectionName);
        }
       
        this.inputCollection("");
        
        // blink on newly created item
        $(".collection-list li").first().addClass("blink-style");
    }

    private update(dto: Raven.Client.Documents.Operations.ETL.Transformation, isNew: boolean, resetScript: boolean) {
        this.name(dto.Name);
        this.script(dto.Script);
        this.transformScriptCollections(dto.Collections || []);
        this.applyScriptForAllCollections(dto.ApplyToAllDocuments);
        
        if (this.applyScriptForAllCollections()) {
            this.transformScriptCollections([ongoingTaskRavenEtlTransformationModel.applyToAllCollectionsText]);
        }
        
        this.isNew(isNew);
        this.resetScript(resetScript)
    }

    hasUpdates(oldItem: this) {
        const hashFunction = jsonUtil.newLineNormalizingHashFunctionWithIgnoredFields(["__moduleId__", "validationGroup"]);
        return hashFunction(this) !== hashFunction(oldItem);
    }
}

export = ongoingTaskRavenEtlTransformationModel;

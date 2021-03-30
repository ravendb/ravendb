/// <reference path="../../../../typings/tsd.d.ts"/>
import collectionsTracker = require("common/helpers/database/collectionsTracker");
import jsonUtil = require("common/jsonUtil");

class ongoingTaskOlapEtlTransformationModel {
    name = ko.observable<string>();
    script = ko.observable<string>();
    collection = ko.observable<string>();    
    
    collectionColorIndex: KnockoutComputed<number>;    
    isNew = ko.observable<boolean>(true);
    resetScript = ko.observable<boolean>(false);
    
    validationGroup: KnockoutValidationGroup; 
  
    dirtyFlag: () => DirtyFlag;
    
    constructor(dto: Raven.Client.Documents.Operations.ETL.Transformation, isNew: boolean, resetScript: boolean) {
        this.update(dto, isNew, resetScript);
        
        this.initObservables();
        this.initValidation();
    }

    initObservables() {
        this.collectionColorIndex = ko.pureComputed(() => collectionsTracker.default.getCollectionColorIndex(this.collection()));
        
        this.dirtyFlag = new ko.DirtyFlag([
            this.name, 
            this.script,
            this.collection,
            this.resetScript
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }
   
    static empty(): ongoingTaskOlapEtlTransformationModel {
        return new ongoingTaskOlapEtlTransformationModel(
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
            ApplyToAllDocuments: false,
            Collections: [this.collection()],
            Disabled: false,
            Name: this.name(),
            Script: this.script()
        }
    }

    private initValidation() {
        this.script.extend({
            required: true,
            aceValidation: true
        });
        
        this.collection.extend({
            required: true
        });

        this.validationGroup = ko.validatedObservable({
            script: this.script,
            collection: this.collection
        });
    }

    update(dto: Raven.Client.Documents.Operations.ETL.Transformation, isNew: boolean, resetScript: boolean) {
        this.name(dto.Name);
        this.script(dto.Script); 
        this.collection(dto.Collections[0]);
        this.isNew(isNew);
        this.resetScript(resetScript);
    }

    getCollectionEntry(collectionName: string) {
        return collectionsTracker.default.getCollectionColorIndex(collectionName);       
    }

    hasUpdates(oldItem: this) {
        const hashFunction = jsonUtil.newLineNormalizingHashFunctionWithIgnoredFields(["__moduleId__", "validationGroup"]);
        return hashFunction(this) !== hashFunction(oldItem);
    }
}

export = ongoingTaskOlapEtlTransformationModel;

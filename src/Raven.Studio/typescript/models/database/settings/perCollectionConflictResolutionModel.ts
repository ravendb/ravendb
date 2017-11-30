/// <reference path="../../../../typings/tsd.d.ts"/>
import jsonUtil = require("common/jsonUtil");

class perCollectionConflictResolutionModel {
    
    collection = ko.observable<string>();
    script = ko.observable<string>();

    validationGroup: KnockoutValidationGroup; 
    
    dirtyFlag: () => DirtyFlag;
  
    constructor(dto: Raven.Client.ServerWide.ScriptResolver) {
        this.update(dto);
        this.initValidation();

        this.dirtyFlag = new ko.DirtyFlag([
            this.collection,
            this.script], 
        false, jsonUtil.newLineNormalizingHashFunction);
    }

    static create(collectionName: string, resolver: Raven.Client.ServerWide.ScriptResolver) {
        const model = new perCollectionConflictResolutionModel({
            Script: resolver.Script,
            LastModifiedTime: resolver.LastModifiedTime 
        });
        model.collection(collectionName);
        model.dirtyFlag().reset();
        
        return model;
    }
    
    static empty() {
        return new perCollectionConflictResolutionModel({
            Script: "",
            LastModifiedTime: null
        });
    }
     
    private initValidation() {
        this.collection.extend({
            required: true
        });
        this.script.extend({
            required: true
        });
        
        this.validationGroup = ko.validatedObservable({
            script: this.script,
            collection: this.collection
        });
    }
    
    toDto(): Raven.Client.ServerWide.ScriptResolver {
        return {
            LastModifiedTime: null, //TODO
            Script: this.script()
        }   
    }
    
    private update(dto: Raven.Client.ServerWide.ScriptResolver) {
        this.script(dto.Script);
    }

    hasUpdates(oldItem: this) {
        const hashFunction = jsonUtil.newLineNormalizingHashFunctionWithIgnoredFields(["__moduleId__", "validationGroup"]);
        return hashFunction(this) !== hashFunction(oldItem);
    } 
}

export = perCollectionConflictResolutionModel;

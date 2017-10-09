/// <reference path="../../../../typings/tsd.d.ts"/>
import collectionsTracker = require("common/helpers/database/collectionsTracker");

class ongoingTaskSqlEtlTransformationModel {
    name = ko.observable<string>();
    script = ko.observable<string>();
    collection = ko.observable<string>();    
    
    collectionColorIndex: KnockoutComputed<number>;    
    isNew = ko.observable<boolean>(true);
    
    validationGroup: KnockoutValidationGroup; 
  
    constructor(dto: Raven.Client.ServerWide.ETL.Transformation, isNew: boolean) {
        this.update(dto, isNew);
        this.initObservables();
    }

    initObservables() {
        this.collectionColorIndex = ko.pureComputed(() => collectionsTracker.default.getCollectionColorIndex(this.collection()));
    }
   
    static empty(): ongoingTaskSqlEtlTransformationModel {
        return new ongoingTaskSqlEtlTransformationModel(
            {
                ApplyToAllDocuments: false, 
                Collections: [],
                Disabled: false,
                HasLoadAttachment: false,
                Name: "",
                Script: ""
            }, true);
    }

    toDto(): Raven.Client.ServerWide.ETL.Transformation {
        return {
            ApplyToAllDocuments: false,
            Collections: [this.collection()],
            Disabled: false,
            HasLoadAttachment: false,
            Name: this.name(),
            Script: this.script()
        }
    }

    private initValidation() {
        this.name.extend({
            required: true
        });
        
        this.script.extend({
            required: true
        });
        
        this.collection.extend({
            required: true
        });

        this.validationGroup = ko.validatedObservable({
            name: this.name,
            script: this.script,
            collection: this.collection
        });
    }

    update(dto: Raven.Client.ServerWide.ETL.Transformation, isNew: boolean) {
        this.name(dto.Name);
        this.script(dto.Script); 
        this.collection(dto.Collections[0]); // todo: check this..
        this.isNew(isNew);

        // Reset validation for this transformation script model 
        this.name.extend({ validatable: false });
        this.script.extend({ validatable: false });
        this.collection.extend({ validatable: false });
        this.initValidation();
    }

    getCollectionEntry(collectionName: string) {
        return collectionsTracker.default.getCollectionColorIndex(collectionName);       
    }
}

export = ongoingTaskSqlEtlTransformationModel;

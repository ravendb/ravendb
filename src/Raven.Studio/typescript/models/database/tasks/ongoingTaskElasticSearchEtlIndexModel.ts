/// <reference path="../../../../typings/tsd.d.ts"/>

import jsonUtil = require("common/jsonUtil");

class ongoingTaskElasticSearchEtlIndexModel {
    indexName = ko.observable<string>();
    idProperty = ko.observable<string>();
    
    isNew = ko.observable<boolean>(true); 
    validationGroup: KnockoutValidationGroup; 
    
    dirtyFlag: () => DirtyFlag;
  
    constructor(dto: Raven.Client.Documents.Operations.ETL.ElasticSearch.ElasticSearchIndex, isNew: boolean) {
        this.update(dto, isNew);
        
        this.initValidation();
        
        this.dirtyFlag = new ko.DirtyFlag([
            this.indexName,
            this.idProperty
        ], false);
    }

    static empty(): ongoingTaskElasticSearchEtlIndexModel {
        return new ongoingTaskElasticSearchEtlIndexModel(
            {
                IndexName: "",
                IndexIdProperty: ""
            }, true);
    }

    toDto(): Raven.Client.Documents.Operations.ETL.ElasticSearch.ElasticSearchIndex {
        return {
            IndexName: this.indexName(),
            IndexIdProperty: this.idProperty()
        }
    }
    
    private initValidation() {
        this.indexName.extend({
            required: true // todo verify lower case...
        });
        
        this.idProperty.extend({
            required: true
        });

        this.validationGroup = ko.validatedObservable({
            indexName: this.indexName,
            idProperty: this.idProperty
        });
    }

    private update(dto: Raven.Client.Documents.Operations.ETL.ElasticSearch.ElasticSearchIndex, isNew: boolean) {
        this.indexName(dto.IndexName);
        this.idProperty(dto.IndexIdProperty);
        this.isNew(isNew);
    }

    hasUpdates(oldItem: this) {
        const hashFunction = jsonUtil.newLineNormalizingHashFunctionWithIgnoredFields(["__moduleId__", "validationGroup"]);
        return hashFunction(this) !== hashFunction(oldItem);
    }
}

export = ongoingTaskElasticSearchEtlIndexModel;

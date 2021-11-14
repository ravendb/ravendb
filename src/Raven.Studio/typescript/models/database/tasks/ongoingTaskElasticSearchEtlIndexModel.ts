/// <reference path="../../../../typings/tsd.d.ts"/>
import jsonUtil = require("common/jsonUtil");
import generalUtils = require("common/generalUtils");
import validateNameCommand = require("commands/resources/validateNameCommand");

class ongoingTaskElasticSearchEtlIndexModel {
    indexName = ko.observable<string>();
    idProperty = ko.observable<string>();
    insertOnlyMode = ko.observable<boolean>();
    
    isNew = ko.observable<boolean>(true); 
    validationGroup: KnockoutValidationGroup; 
    
    dirtyFlag: () => DirtyFlag;
  
    constructor(dto: Raven.Client.Documents.Operations.ETL.ElasticSearch.ElasticSearchIndex, isNew: boolean) {
        this.update(dto, isNew);
        
        this.initValidation();
        
        this.dirtyFlag = new ko.DirtyFlag([
            this.indexName,
            this.idProperty,
            this.insertOnlyMode
        ], false);
    }

    static empty(): ongoingTaskElasticSearchEtlIndexModel {
        return new ongoingTaskElasticSearchEtlIndexModel(
            {
                IndexName: "",
                DocumentIdProperty: "",
                InsertOnlyMode: false
            }, true);
    }

    toDto(): Raven.Client.Documents.Operations.ETL.ElasticSearch.ElasticSearchIndex {
        return {
            IndexName: this.indexName(),
            DocumentIdProperty: this.idProperty(),
            InsertOnlyMode: this.insertOnlyMode()
        }
    }
    
    private initValidation() {

        const checkIndexName = (val: string,
                                params: any,
                                callback: (currentValue: string, errorMessageOrValidationResult: string | boolean) => void) => {
            new validateNameCommand('ElasticSearchIndex', val)
                .execute()
                .done((result) => {
                    if (result.IsValid) {
                        callback(this.indexName(), true);
                    } else {
                        callback(this.indexName(), result.ErrorMessage);
                    }
                })
        };
        
        this.indexName.extend({
            required: true,
            validation: [
                {
                    async: true,
                    validator: generalUtils.debounceAndFunnel(checkIndexName)
                }
            ]
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
        this.idProperty(dto.DocumentIdProperty);
        this.insertOnlyMode(dto.InsertOnlyMode);
        this.isNew(isNew);
    }

    hasUpdates(oldItem: this) {
        const hashFunction = jsonUtil.newLineNormalizingHashFunctionWithIgnoredFields(["__moduleId__", "validationGroup"]);
        return hashFunction(this) !== hashFunction(oldItem);
    }
}

export = ongoingTaskElasticSearchEtlIndexModel;

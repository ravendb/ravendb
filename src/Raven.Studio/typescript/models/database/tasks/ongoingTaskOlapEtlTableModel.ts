/// <reference path="../../../../typings/tsd.d.ts"/>

import jsonUtil = require("common/jsonUtil");

class ongoingTaskOlapEtlTableModel {
    tableName = ko.observable<string>();
    documentIdColumn = ko.observable<string>();
    
    isNew = ko.observable<boolean>(true);
    validationGroup: KnockoutValidationGroup; 
    
    dirtyFlag: () => DirtyFlag;
  
    constructor(dto: Raven.Client.Documents.Operations.ETL.OLAP.OlapEtlTable, isNew: boolean) {
        this.update(dto, isNew);
        
        this.initValidation();
        
        this.dirtyFlag = new ko.DirtyFlag([
            this.tableName,
            this.documentIdColumn
        ], false);
    }

    static empty(): ongoingTaskOlapEtlTableModel {
        return new ongoingTaskOlapEtlTableModel(
            {
                TableName: "",
                DocumentIdColumn: ""
            }, true);
    }

    hasContent() {
        return !!(this.tableName() || this.documentIdColumn());
    }

    toDto(): Raven.Client.Documents.Operations.ETL.OLAP.OlapEtlTable {
        return {
            TableName: this.tableName(),
            DocumentIdColumn: this.documentIdColumn()
        }
    }
    
    private initValidation() {
        this.tableName.extend({
            required: true
        });
        
        this.documentIdColumn.extend({
            required: true
        });

        this.validationGroup = ko.validatedObservable({
            tableName: this.tableName,
            documentIdColumn: this.documentIdColumn
        });
    }

    private update(dto: Raven.Client.Documents.Operations.ETL.OLAP.OlapEtlTable, isNew: boolean) {
        this.tableName(dto.TableName);
        this.documentIdColumn(dto.DocumentIdColumn);
        this.isNew(isNew);
    }

    hasUpdates(oldItem: this) {
        const hashFunction = jsonUtil.newLineNormalizingHashFunctionWithIgnoredFields(["__moduleId__", "validationGroup"]);
        return hashFunction(this) !== hashFunction(oldItem);
    }
}

export = ongoingTaskOlapEtlTableModel;

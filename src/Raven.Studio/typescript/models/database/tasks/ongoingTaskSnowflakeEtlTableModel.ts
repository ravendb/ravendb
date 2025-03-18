/// <reference path="../../../../typings/tsd.d.ts"/>

import jsonUtil = require("common/jsonUtil");
class ongoingTaskSnowflakeEtlTableModel {
    tableName = ko.observable<string>();
    documentIdColumn = ko.observable<string>();
    insertOnlyMode = ko.observable<boolean>();
    
    isNew = ko.observable<boolean>(true); 
    validationGroup: KnockoutValidationGroup; 
    
    dirtyFlag: () => DirtyFlag;
  
    constructor(dto: Raven.Client.Documents.Operations.ETL.Snowflake.SnowflakeEtlTable, isNew: boolean) {
        this.update(dto, isNew);
        
        this.initValidation();
        
        this.dirtyFlag = new ko.DirtyFlag([
            this.tableName,
            this.documentIdColumn,
            this.insertOnlyMode
        ], false);
    }

    static empty(): ongoingTaskSnowflakeEtlTableModel {
        return new ongoingTaskSnowflakeEtlTableModel(
            {
                TableName: "",
                DocumentIdColumn: "",
                InsertOnlyMode: false
            }, true);
    }

    toDto(): Raven.Client.Documents.Operations.ETL.Snowflake.SnowflakeEtlTable {
        return {
            TableName: this.tableName(),
            DocumentIdColumn: this.documentIdColumn(),
            InsertOnlyMode: this.insertOnlyMode()
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

    private update(dto: Raven.Client.Documents.Operations.ETL.Snowflake.SnowflakeEtlTable, isNew: boolean) {
        this.tableName(dto.TableName);
        this.documentIdColumn(dto.DocumentIdColumn); 
        this.insertOnlyMode(dto.InsertOnlyMode);
        this.isNew(isNew);       
    }

    hasUpdates(oldItem: this) {
        const hashFunction = jsonUtil.newLineNormalizingHashFunctionWithIgnoredFields(["__moduleId__", "validationGroup"]);
        return hashFunction(this) !== hashFunction(oldItem);
    }
    
}

export = ongoingTaskSnowflakeEtlTableModel;

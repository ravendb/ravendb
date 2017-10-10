/// <reference path="../../../../typings/tsd.d.ts"/>

class ongoingTaskSqlEtlTableModel {
    tableName = ko.observable<string>();
    documentIdColumn = ko.observable<string>();
    insertOnlyMode = ko.observable<boolean>();
    
    isNew = ko.observable<boolean>(true); 
    validationGroup: KnockoutValidationGroup; 
  
    constructor(dto: Raven.Client.ServerWide.ETL.SqlEtlTable, isNew: boolean) {
        this.update(dto, isNew);
        
        this.initValidation();
    }

    static empty(): ongoingTaskSqlEtlTableModel {
        return new ongoingTaskSqlEtlTableModel(
            {
                TableName: "",
                DocumentIdColumn: "",
                InsertOnlyMode: false
            }, true);
    }

    toDto(): Raven.Client.ServerWide.ETL.SqlEtlTable {
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

    update(dto: Raven.Client.ServerWide.ETL.SqlEtlTable, isNew: boolean) {
        this.tableName(dto.TableName);
        this.documentIdColumn(dto.DocumentIdColumn); 
        this.insertOnlyMode(dto.InsertOnlyMode);
        this.isNew(isNew);       
    }
}

export = ongoingTaskSqlEtlTableModel;

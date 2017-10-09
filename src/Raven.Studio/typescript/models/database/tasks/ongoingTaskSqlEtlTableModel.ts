/// <reference path="../../../../typings/tsd.d.ts"/>

class OngoingTaskSqlEtlTableModel {
    tableName = ko.observable<string>();
    primaryKey = ko.observable<string>();
    insertOnlyMode = ko.observable<boolean>();
    
    isNew = ko.observable<boolean>(true); 
    validationGroup: KnockoutValidationGroup; 
  
    constructor(dto: Raven.Client.ServerWide.ETL.SqlEtlTable, isNew: boolean) {
        this.update(dto, isNew);
    }

    static empty(): OngoingTaskSqlEtlTableModel {
        return new OngoingTaskSqlEtlTableModel(
            {
                TableName: "",
                DocumentIdColumn: "",
                InsertOnlyMode: false
            }, true);
    }

    toDto(): Raven.Client.ServerWide.ETL.SqlEtlTable {
        return {
            TableName: this.tableName(),
            DocumentIdColumn: this.primaryKey(),
            InsertOnlyMode: this.insertOnlyMode()
        }
    }

    private initValidation() {
        this.tableName.extend({
            required: true
        });
        
        this.primaryKey.extend({
            required: true
        });               

        this.validationGroup = ko.validatedObservable({
            tableName: this.tableName,
            primaryKey: this.primaryKey
        });
    }

    update(dto: Raven.Client.ServerWide.ETL.SqlEtlTable, isNew: boolean) {
        this.tableName(dto.TableName);
        this.primaryKey(dto.DocumentIdColumn); 
        this.insertOnlyMode(dto.InsertOnlyMode);
        this.isNew(isNew);

        // Reset validation for this table model 
        this.tableName.extend({ validatable: false });
        this.primaryKey.extend({ validatable: false });        
        this.initValidation();
    }
}

export = OngoingTaskSqlEtlTableModel;

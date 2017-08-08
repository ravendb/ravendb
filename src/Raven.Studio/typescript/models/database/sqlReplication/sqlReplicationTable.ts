/// <reference path="../../../../typings/tsd.d.ts"/>

class sqlReplicationTable {

    tableName = ko.observable<string>().extend({ required: true });
    documentIdColumn = ko.observable<string>().extend({ required: true });
    insertOnly = ko.observable<boolean>(false);
   
    constructor(dto: Raven.Client.ServerWide.ETL.SqlEtlTable) {
        this.tableName(dto.TableName);
        this.documentIdColumn(dto.DocumentIdColumn);
        this.insertOnly(dto.InsertOnlyMode);
    }

    static empty(): sqlReplicationTable {
        return new sqlReplicationTable({
            TableName: null,
            DocumentIdColumn: null,
            InsertOnlyMode: false
        });
    }
   
    toDto(): Raven.Client.ServerWide.ETL.SqlEtlTable {
        return {
            TableName: this.tableName(),
            DocumentIdColumn: this.documentIdColumn(),
            InsertOnlyMode: this.insertOnly()
        };
    }
}

export = sqlReplicationTable;

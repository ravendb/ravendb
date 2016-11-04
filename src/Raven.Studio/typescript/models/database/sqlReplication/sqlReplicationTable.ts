/// <reference path="../../../../typings/tsd.d.ts"/>

class sqlReplicationTable {

    tableName = ko.observable<string>().extend({ required: true });
    documentKeyColumn = ko.observable<string>().extend({ required: true });
    insertOnly = ko.observable<boolean>(false);

    constructor(dto: Raven.Server.Documents.SqlReplication.SqlReplicationTable) {
        this.tableName(dto.TableName);
        this.documentKeyColumn(dto.DocumentKeyColumn);
        this.insertOnly(dto.InsertOnlyMode);
    }

    static empty(): sqlReplicationTable {
        return new sqlReplicationTable({
            TableName: null,
            DocumentKeyColumn: null,
            InsertOnlyMode: false
        });
    }

    toDto(): Raven.Server.Documents.SqlReplication.SqlReplicationTable {
        return {
            TableName: this.tableName(),
            DocumentKeyColumn: this.documentKeyColumn(),
            InsertOnlyMode: this.insertOnly()
        };
    }
}

export = sqlReplicationTable;

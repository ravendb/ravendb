/// <reference path="../../../../typings/tsd.d.ts"/>

class sqlReplicationTable {

    tableName = ko.observable<string>().extend({ required: true });
    documentKeyColumn = ko.observable<string>().extend({ required: true });
    insertOnly = ko.observable<boolean>(false);

    constructor(dto: sqlReplicationTableDto) {
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

    toDto(): sqlReplicationTableDto {
        return {
            TableName: this.tableName(),
            DocumentKeyColumn: this.documentKeyColumn(),
            InsertOnlyMode: this.insertOnly()
        };
    }
}

export = sqlReplicationTable;

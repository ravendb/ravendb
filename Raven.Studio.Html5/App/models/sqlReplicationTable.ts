class sqlReplicationTable {

    tableName = ko.observable<string>().extend({ required: true });
    documentKeyColumn = ko.observable<string>().extend({ required: true });

    constructor(dto: sqlReplicationTableDto) {
        this.tableName(dto.TableName);
        this.documentKeyColumn(dto.DocumentKeyColumn);
    }

    static empty(): sqlReplicationTable {
        return new sqlReplicationTable({
            TableName: null,
            DocumentKeyColumn: null
        });
    }

    toDto(): sqlReplicationTableDto {
        return {
            TableName: this.tableName(),
            DocumentKeyColumn: this.documentKeyColumn()
        };
    }
}

export = sqlReplicationTable;
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

    isValid(): boolean {
        var requiredValues = [this.tableName(), this.documentKeyColumn()];
        return requiredValues.every(v => v != null && v.length > 0);
    }
}

export = sqlReplicationTable;
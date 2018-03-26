/// <reference path="../../../../../typings/tsd.d.ts"/>

class sqlColumn {
    sqlName: string;
    propertyName = ko.observable<string>();
    type: Raven.Server.SqlMigration.Schema.ColumnType;
    
    constructor(column: Raven.Server.SqlMigration.Schema.TableColumn) {
        this.sqlName = column.Name;
        this.propertyName(column.Name);
        this.type = column.Type;
    }
}


export = sqlColumn;

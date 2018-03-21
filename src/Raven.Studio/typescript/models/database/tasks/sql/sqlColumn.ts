/// <reference path="../../../../../typings/tsd.d.ts"/>

class sqlColumn {
    name: string;
    type: Raven.Server.SqlMigration.Schema.ColumnType;
    
    constructor(column: Raven.Server.SqlMigration.Schema.TableColumn) {
        this.name = column.Name;
        this.type = column.Type;
    }
}


export = sqlColumn;

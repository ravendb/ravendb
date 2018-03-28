/// <reference path="../../../../../typings/tsd.d.ts"/>

class sqlColumn {
    sqlName: string;
    propertyName = ko.observable<string>();
    type: Raven.Server.SqlMigration.Schema.ColumnType;
    
    constructor(column: Raven.Server.SqlMigration.Schema.TableColumn) {
        this.sqlName = column.Name;
        this.propertyName(_.upperFirst(_.camelCase(column.Name))); //tODO: use settings for doing this
        this.type = column.Type;
    }
    
    clone() {
        return new sqlColumn({
            Name: this.sqlName,
            Type: this.type
        });
    }
}


export = sqlColumn;

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
    
    clone() {
        return new sqlColumn({
            Name: this.sqlName,
            Type: this.type
        });
    }
    
    columnTypeAsHtml(binaryAsAttachment: boolean) {
        if (binaryAsAttachment && this.type === "Binary") {
            return ': <i title="Attachment" class="icon-attachment"></i>';
        }
        return ': ' + this.type;
    }

    isUnsupported() {
        return this.type === "Unsupported";
    }
}


export = sqlColumn;

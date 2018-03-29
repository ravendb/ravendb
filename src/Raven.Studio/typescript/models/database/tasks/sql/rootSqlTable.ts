/// <reference path="../../../../../typings/tsd.d.ts"/>

import abstractSqlTable = require("models/database/tasks/sql/abstractSqlTable");
import innerSqlTable = require("models/database/tasks/sql/innerSqlTable");

class rootSqlTable extends abstractSqlTable {
    collectionName = ko.observable<string>();
    checked = ko.observable<boolean>(true);
    documentIdTemplate: KnockoutComputed<string>;
    
    query = ko.observable<string>();
    patchScript = ko.observable<string>();
    
    constructor() {
        super();
        this.documentIdTemplate = ko.pureComputed(() => {
            const templetePart = this.primaryKeyColumns().map(x => '{' + x.sqlName + '}').join("/");
            return this.collectionName() + "/" + templetePart;
        });
    }
    
    toDto() {
        return {
            SourceTableName: this.tableName,
            SourceTableSchema: this.tableSchema,
            Name: this.collectionName(),
            Patch: this.patchScript(),
            SourceTableQuery: this.query(),
            NestedCollections: this.getEmbeddedReferencesDto(),
            LinkedCollections: this.getLinkedReferencesDto(),
            ColumnsMapping: this.getColumnsMapping(),
        } as Raven.Server.SqlMigration.Model.RootCollection;
    }
    
    cloneForEmbed(): innerSqlTable {
        const table = new innerSqlTable(this);
        
        table.tableName = this.tableName;
        table.tableSchema = this.tableSchema;
        table.documentColumns(this.documentColumns().map(x => x.clone()));
        table.primaryKeyColumns(this.primaryKeyColumns().map(x => x.clone()));
        table.references(this.references().map(x => x.clone()));
        return table;
    }
}


export = rootSqlTable;
 

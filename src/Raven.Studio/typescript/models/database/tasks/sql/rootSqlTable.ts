/// <reference path="../../../../../typings/tsd.d.ts"/>

import abstractSqlTable = require("models/database/tasks/sql/abstractSqlTable");
import innerSqlTable = require("models/database/tasks/sql/innerSqlTable");
import sqlReference = require("models/database/tasks/sql/sqlReference");

class rootSqlTable extends abstractSqlTable {
    collectionName = ko.observable<string>();
    checked = ko.observable<boolean>(true);
    documentIdTemplate: KnockoutComputed<string>;
    
    customizeQuery = ko.observable<boolean>(false); //TODO: validation  required onlyif
    query = ko.observable<string>();
    //TODO: autocomplete columns names?
    transformResults = ko.observable<boolean>(false);  //TODO: validation  required onlyif
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
            Patch: this.transformResults() ? this.patchScript() : undefined,
            SourceTableQuery: this.customizeQuery() ? this.query() : undefined,
            NestedCollections: this.getEmbeddedReferencesDto(),
            LinkedCollections: this.getLinkedReferencesDto(),
            ColumnsMapping: this.getColumnsMapping(),
        } as Raven.Server.SqlMigration.Model.RootCollection;
    }
    
    cloneForEmbed(parentReference: sqlReference): innerSqlTable {
        const table = new innerSqlTable(parentReference);
        
        table.tableName = this.tableName;
        table.tableSchema = this.tableSchema;
        table.documentColumns(this.documentColumns().map(x => x.clone()));
        table.primaryKeyColumns(this.primaryKeyColumns().map(x => x.clone()));
        table.references(this.references().map(x => {
            const clonedObject = x.clone();
            clonedObject.sourceTable = table;
            return clonedObject;
        }));
        
        return table;
    }
}


export = rootSqlTable;
 

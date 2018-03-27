/// <reference path="../../../../../typings/tsd.d.ts"/>

import abstractSqlTable = require("models/database/tasks/sql/abstractSqlTable");

class rootSqlTable extends abstractSqlTable {
    collectionName = ko.observable<string>();
    checked = ko.observable<boolean>(true);
    documentIdTemplate: KnockoutComputed<string>;
    
    constructor() {
        super();
        this.documentIdTemplate = ko.pureComputed(() => {
            const templetePart = this.primaryKeyColumns().map(x => '{' + x.sqlName + '}').join("/");
            return this.collectionName() + "/" + templetePart;
        });
    }
    
    toDto() {
        const linkedReferences = this.references()
            .filter(x => x.action() === 'link')
            .map(x => {
                return {
                    Name: x.name(),
                    SourceTableName: x.targetTable.tableName,
                    SourceTableSchema: x.targetTable.tableSchema,
                    JoinColumns: x.joinColumns,
                    Type: x.type
                } as Raven.Server.SqlMigration.Model.LinkedCollection; 
            });
        
        const mapping = {} as dictionary<string>;
        this.documentColumns().forEach(column => {
            mapping[column.sqlName] = column.propertyName();
        });
        
        return {
            SourceTableName: this.tableName,
            SourceTableSchema: this.tableSchema,
            Name: this.collectionName(),
            Patch: null, //TODO:
            SourceTableQuery: null,  //TODO
            NestedCollections: [], //TODO
            LinkedCollections:  linkedReferences,
            ColumnsMapping: mapping
        } as Raven.Server.SqlMigration.Model.RootCollection;
    }
}


export = rootSqlTable;
 

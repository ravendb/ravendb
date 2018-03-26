/// <reference path="../../../../../typings/tsd.d.ts"/>

import sqlColumn = require("models/database/tasks/sql/sqlColumn");
import sqlReference = require("models/database/tasks/sql/sqlReference");

class sqlTable { //TODO: split to sqlRootTable and sqlInnerTable
    tableSchema: string;
    tableName: string;
    customCollection = ko.observable<string>();
    primaryKeyColumns = ko.observableArray<sqlColumn>([]);
    documentColumns = ko.observableArray<sqlColumn>([]);
    checked = ko.observable<boolean>(true);
    references = ko.observableArray<sqlReference>([]);
    
    documentIdTemplate: KnockoutComputed<string>;
    
    constructor() {
        this.documentIdTemplate = ko.pureComputed(() => {
            const templetePart = this.primaryKeyColumns().map(x => '{' + x.sqlName + '}').join("/");
            return this.customCollection() + "/" + templetePart;
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
            Name: this.customCollection(),
            Patch: null, //TODO:
            SourceTableQuery: null,  //TODO
            NestedCollections: [], //TODO
            LinkedCollections:  linkedReferences,
            ColumnsMapping: mapping
        } as Raven.Server.SqlMigration.Model.RootCollection;
    }
}


export = sqlTable;
 

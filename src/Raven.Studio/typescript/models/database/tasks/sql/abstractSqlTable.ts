/// <reference path="../../../../../typings/tsd.d.ts"/>

import sqlColumn = require("models/database/tasks/sql/sqlColumn");
import sqlReference = require("models/database/tasks/sql/sqlReference");

abstract class abstractSqlTable {
    tableSchema: string;
    tableName: string;

    primaryKeyColumns = ko.observableArray<sqlColumn>([]);
    documentColumns = ko.observableArray<sqlColumn>([]);
    references = ko.observableArray<sqlReference>([]);

    getPrimaryKeyColumnNames() {
        return this.primaryKeyColumns().map(x => x.sqlName);
    }
    
    getLinkedReferencesDto() {
         return this.references()
            .filter(x => x.action() === 'link')
            .map(x => x.toLinkDto());
    }
    
    getEmbeddedReferencesDto() {
        return this.references()
            .filter(x => x.action() === 'embed')
            .map(x => x.toEmbeddedDto());
    }
    
    getColumnsMapping() {
        const mapping = {} as dictionary<string>;
        this.documentColumns().forEach(column => {
            mapping[column.sqlName] = column.propertyName();
        });
        return mapping;
    }
    
    findLinksToTable(tableToFind: abstractSqlTable): Array<sqlReference> {
        const foundItems = this.references()
            .filter(x => x.action() === 'link' && x.targetTable.tableSchema === tableToFind.tableSchema && x.targetTable.tableName === tableToFind.tableName);
        
        this.references()
            .filter(x => x.action() === "embed")
            .map(ref => {
                foundItems.push(...ref.effectiveInnerTable().findLinksToTable(tableToFind));
            });
        
        return foundItems;
    }
    
}

export = abstractSqlTable;

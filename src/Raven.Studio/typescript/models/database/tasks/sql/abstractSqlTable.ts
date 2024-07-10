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
    
    getEmbeddedReferencesDto(binaryToAttachment: boolean) {
        return this.references()
            .filter(x => x.action() === 'embed')
            .map(x => x.toEmbeddedDto(binaryToAttachment));
    }
    
    checkForDuplicateProperties() {
        const localProperties = this.documentColumns().map(x => x.propertyName());
        return localProperties.length !== _.uniq(localProperties).length;
    }
    
    getColumnsMapping(binaryToAttachment: boolean) {
        const mapping: dictionary<string> = {};
        this.documentColumns()
            .filter(x => binaryToAttachment ? x.type !== "Binary" : true)
            .forEach(column => {
                mapping[column.sqlName] = column.propertyName();
            });
        return mapping;
    }
    
    getAttachmentsMapping(binaryToAttachment: boolean) {
        const mapping: dictionary<string> = {};
        
        if (!binaryToAttachment) {
            return mapping;
        }
        
        this.documentColumns()
            .filter(x => x.type === "Binary")
            .forEach(column => {
                mapping[column.sqlName] = column.propertyName();
            });
        return mapping;
    }
    
    findReference(target: Raven.Server.SqlMigration.Model.ICollectionReference): sqlReference {
        return this.references().find(x => x.type === target.Type 
            && _.isEqual(x.joinColumns, target.JoinColumns) 
            && x.targetTable.tableSchema === target.SourceTableSchema 
            && x.targetTable.tableName === target.SourceTableName);
    }
    
    findLinksToTable(tableToFind: abstractSqlTable): Array<sqlReference> {
        const foundItems = this.references()
            .filter(x => x.action() === 'link' && x.targetTable.tableSchema === tableToFind.tableSchema && x.targetTable.tableName === tableToFind.tableName);
        
        this.references()
            .filter(x => x.action() === "embed")
            .map(ref => {
                if (ref.effectiveInnerTable()) {
                    foundItems.push(...ref.effectiveInnerTable().findLinksToTable(tableToFind));
                }
            });
        
        return foundItems;
    }
    
    
    isManyToMany() {
        if (this.references().length !== 2) {
            // many-to-many should have 2 references
            return false;
        }
        
        if (this.references().some(x => x.type !== "ManyToOne")) {
            // each reference should be manyToOne
            return false;
        }
        
        // at this point we have 2 many-to-one references
        const allJoinColumns = this.references().flatMap(r => r.joinColumns);
        const primaryColumns = this.getPrimaryKeyColumnNames();
        
        return _.isEqual(allJoinColumns.sort(), primaryColumns.sort()); // references covers all primary key columns
    }
    
     setAllLinksToSkip() {
        this.references().forEach(reference => {
            if (reference.action() === 'link') {
                reference.skip();
            }
            
            if (reference.action() === 'embed') {
                reference.effectiveInnerTable().setAllLinksToSkip();
            }
        });
    }
    
}

export = abstractSqlTable;

/// <reference path="../../../../../typings/tsd.d.ts"/>

import abstractSqlTable = require("models/database/tasks/sql/abstractSqlTable");
import innerSqlTable = require("models/database/tasks/sql/innerSqlTable");
import rootSqlTable = require("models/database/tasks/sql/rootSqlTable");

class sqlReference {
    
    id = _.uniqueId("sql-ref-");
    
    targetTable: abstractSqlTable;
    sourceTable: abstractSqlTable;
    
    name = ko.observable<string>();
    
    action = ko.observable<sqlMigrationAction>();
    
    joinColumns: string[];
    type: Raven.Server.SqlMigration.Model.RelationType;
    
    effectiveInnerTable = ko.observable<innerSqlTable>();
    effectiveLinkTable = ko.observable<abstractSqlTable>();
    
    constructor(targetTable: abstractSqlTable, sourceTable: abstractSqlTable, joinColumns: string[], type: Raven.Server.SqlMigration.Model.RelationType) {
        this.targetTable = targetTable;
        this.sourceTable = sourceTable;
        this.joinColumns = joinColumns;
        this.type = type;
    }
    
    toLinkDto(): Raven.Server.SqlMigration.Model.LinkedCollection {
        return {
            Name: this.name(),
            SourceTableName: this.effectiveLinkTable().tableName,
            SourceTableSchema: this.effectiveLinkTable().tableSchema,
            JoinColumns: this.joinColumns,
            Type: this.type,
            AttachmentNameMapping: undefined,
            ColumnsMapping: undefined
        }; 
    }
    
    toEmbeddedDto(binaryToAttachment: boolean): Raven.Server.SqlMigration.Model.EmbeddedCollection {
        return {
            Name: this.name(),
            SourceTableSchema: this.effectiveInnerTable().tableSchema,
            SourceTableName: this.effectiveInnerTable().tableName,
            Type: this.type, 
            JoinColumns: this.joinColumns,
            ColumnsMapping: this.effectiveInnerTable().getColumnsMapping(binaryToAttachment),
            AttachmentNameMapping: this.effectiveInnerTable().getAttachmentsMapping(binaryToAttachment),
            LinkedCollections: this.effectiveInnerTable().getLinkedReferencesDto(),
            NestedCollections: this.effectiveInnerTable().getEmbeddedReferencesDto(binaryToAttachment),
            SqlKeysStorage: this.effectiveInnerTable().sqlKeysStorage()
        };
    }
    
    clone(): sqlReference {
        const newReference = new sqlReference(this.targetTable, this.sourceTable, this.joinColumns, this.type);
        newReference.skip();
        return newReference;
    }
    
    skip() {
        this.action("skip");
        this.effectiveLinkTable(null);
        this.effectiveInnerTable(null);
    }
    
    link(tableToLink: abstractSqlTable) {
        this.action("link");
        this.effectiveLinkTable(tableToLink);
        this.effectiveInnerTable(null);
    }
    
    embed(innerTable: innerSqlTable) {
        this.action("embed");
        this.effectiveInnerTable(innerTable);
        this.effectiveLinkTable(null);    
    }
    
    getTypeClass() {
        switch (this.type) {
            case "OneToMany":
                return "icon-sql-one-to-many";
            case "ManyToOne":
                return "icon-sql-many-to-one";
        }
    }
    
    canLinkTargetTable() {
        return (this.targetTable as rootSqlTable).checked(); 
    }
}


export = sqlReference;
 

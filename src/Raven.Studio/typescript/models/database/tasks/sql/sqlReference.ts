/// <reference path="../../../../../typings/tsd.d.ts"/>

import abstractSqlTable = require("models/database/tasks/sql/abstractSqlTable");
import innerSqlTable = require("models/database/tasks/sql/innerSqlTable");

class sqlReference {
    
    targetTable: abstractSqlTable;
    
    name = ko.observable<string>();
    
    action = ko.observable<sqlMigrationAction>();
    
    joinColumns: string[];
    type: Raven.Server.SqlMigration.Model.RelationType;
    
    effectiveInnerTable = ko.observable<innerSqlTable>();
    effectiveLinkTable = ko.observable<abstractSqlTable>();
    
    constructor(targetTable: abstractSqlTable, joinColumns: string[], type: Raven.Server.SqlMigration.Model.RelationType) {
        this.targetTable = targetTable;
        this.name(joinColumns.join("And")); //TODO: - consider using collection name by default ? 
        this.joinColumns = joinColumns;
        this.type = type;
        this.action(type === "OneToMany" ? 'skip' : 'link');
    }
    
    toLinkDto(): Raven.Server.SqlMigration.Model.LinkedCollection {
        return {
            Name: this.name(),
            SourceTableName: this.targetTable.tableName,
            SourceTableSchema: this.targetTable.tableSchema,
            JoinColumns: this.joinColumns,
            Type: this.type
        } as Raven.Server.SqlMigration.Model.LinkedCollection; 
    }
    
    toEmbeddedDto(): Raven.Server.SqlMigration.Model.EmbeddedCollection {
        return {
            Name: this.name(),
            SourceTableSchema: this.targetTable.tableSchema,
            SourceTableName: this.targetTable.tableName,
            Type: this.type, 
            JoinColumns: this.joinColumns,
            ColumnsMapping: this.targetTable.getColumnsMapping(),
            LinkedCollections: this.targetTable.getLinkedReferencesDto(),
            NestedCollections: this.targetTable.getEmbeddedReferencesDto()
        } as Raven.Server.SqlMigration.Model.EmbeddedCollection;
    }
    
    clone(): sqlReference {
        return new sqlReference(this.targetTable, this.joinColumns, this.type);
    }
    
    getTypeClass() {
        switch (this.type) {
            case "OneToMany":
                return "icon-sql-one-to-many";
            case "ManyToOne":
                return "icon-sql-many-to-one";
        }
    }
}


export = sqlReference;
 

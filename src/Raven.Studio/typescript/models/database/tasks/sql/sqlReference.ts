/// <reference path="../../../../../typings/tsd.d.ts"/>

import abstractSqlTable = require("models/database/tasks/sql/abstractSqlTable");

class sqlReference {
    
    targetTable: abstractSqlTable;
    
    name = ko.observable<string>();
    
    action = ko.observable<sqlMigrationAction>();
    
    joinColumns: string[];
    type: Raven.Server.SqlMigration.Model.RelationType;
    
    constructor(targetTable: abstractSqlTable, joinColumns: string[], type: Raven.Server.SqlMigration.Model.RelationType) {
        this.targetTable = targetTable;
        this.name(joinColumns.join("And")); //TODO: - consider using collection name by default ? 
        this.joinColumns = joinColumns;
        this.type = type;
        this.action(type === "OneToMany" ? 'skip' : 'link');
    }
}


export = sqlReference;
 

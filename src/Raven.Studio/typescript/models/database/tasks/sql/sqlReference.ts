/// <reference path="../../../../../typings/tsd.d.ts"/>

import sqlTable = require("./sqlTable");

class sqlReference {
    
    targetTable: sqlTable;
    
    name = ko.observable<string>();
    
    action = ko.observable<sqlMigrationAction>();
    
    joinColumns: string[];
    type: Raven.Server.SqlMigration.Model.RelationType;
    
    constructor(targetTable: sqlTable, joinColumns: string[], type: Raven.Server.SqlMigration.Model.RelationType) {
        this.targetTable = targetTable;
        this.name(joinColumns.join("And")); //TODO: - consider using collection name by default ? 
        this.joinColumns = joinColumns;
        this.type = type;
        this.action(type === "OneToMany" ? 'skip' : 'link');
    }
}


export = sqlReference;
 

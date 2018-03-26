/// <reference path="../../../../../typings/tsd.d.ts"/>

import sqlTable = require("./sqlTable");

class sqlReference {
    
    targetTable: sqlTable;
    
    name = ko.observable<string>();
    
    action = ko.observable<sqlMigrationAction>();
    
    columns: string[];
    type: Raven.Server.SqlMigration.Model.RelationType;
    
    constructor(targetTable: sqlTable, columns: string[], type: Raven.Server.SqlMigration.Model.RelationType) {
        this.targetTable = targetTable;
        this.name(columns.join("And")); //TODO: - consider using collection name by default ? 
        this.columns = columns;
        this.type = type;
        this.action(type === "OneToMany" ? 'skip' : 'link');
    }
}


export = sqlReference;
 

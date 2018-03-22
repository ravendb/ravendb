/// <reference path="../../../../../typings/tsd.d.ts"/>

import sqlTable = require("./sqlTable");

type referenceType = "oneToMany" | "manyToOne";

class sqlReference {
    
    targetTable: sqlTable;
    
    name = ko.observable<string>();
    
    action = ko.observable<sqlMigrationAction>();
    
    columns: string[];
    type: referenceType;
    
    constructor(targetTable: sqlTable, columns: string[], type: referenceType) {
        this.targetTable = targetTable;
        this.name(targetTable.tableName);
        this.columns = columns;
        this.type = type;
        this.action(type === "oneToMany" ? 'skip' : 'link');
    }
}


export = sqlReference;
 

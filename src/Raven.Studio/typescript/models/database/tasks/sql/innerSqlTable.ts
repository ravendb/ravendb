/// <reference path="../../../../../typings/tsd.d.ts"/>

import abstractSqlTable = require("models/database/tasks/sql/abstractSqlTable");
import sqlReference = require("models/database/tasks/sql/sqlReference");

class innerSqlTable extends abstractSqlTable {
    parentReference: sqlReference;
    
    constructor(parentReference: sqlReference) {
        super();
        this.parentReference = parentReference;
    }
}


export = innerSqlTable;
 

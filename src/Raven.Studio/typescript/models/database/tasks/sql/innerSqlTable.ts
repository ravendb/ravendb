/// <reference path="../../../../../typings/tsd.d.ts"/>

import abstractSqlTable = require("models/database/tasks/sql/abstractSqlTable");

class innerSqlTable extends abstractSqlTable { 
    parent: abstractSqlTable;
    
    constructor(parent: abstractSqlTable) {
        super();
        this.parent = parent;
    }
}


export = innerSqlTable;
 

/// <reference path="../../../../../typings/tsd.d.ts"/>

import abstractSqlTable = require("models/database/tasks/sql/abstractSqlTable");
import sqlReference = require("models/database/tasks/sql/sqlReference");

class innerSqlTable extends abstractSqlTable {
    parentReference: sqlReference;

    sqlKeysStorage = ko.observable<Raven.Server.SqlMigration.Model.EmbeddedDocumentSqlKeysStorage>("None");
    
    constructor(parentReference: sqlReference) {
        super();
        this.parentReference = parentReference;
    }
    
    removeBackReference(reference: sqlReference) {
        const refToDelete = this.references().find(t => _.isEqual(t.joinColumns, reference.joinColumns)
            && t.targetTable.tableName === reference.sourceTable.tableName
            && t.targetTable.tableSchema === reference.targetTable.tableSchema);

        this.references.remove(refToDelete);
    }
}


export = innerSqlTable;
 

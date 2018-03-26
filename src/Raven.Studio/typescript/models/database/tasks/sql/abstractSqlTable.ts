/// <reference path="../../../../../typings/tsd.d.ts"/>

import sqlColumn = require("models/database/tasks/sql/sqlColumn");
import sqlReference = require("models/database/tasks/sql/sqlReference");

abstract class abstractSqlTable {
    tableSchema: string;
    tableName: string;
    
    customCollection = ko.observable<string>();
    primaryKeyColumns = ko.observableArray<sqlColumn>([]);
    documentColumns = ko.observableArray<sqlColumn>([]);
    references = ko.observableArray<sqlReference>([]);
    
    abstract toDto(): Raven.Server.SqlMigration.Model.AbstractCollection;
}

export = abstractSqlTable;

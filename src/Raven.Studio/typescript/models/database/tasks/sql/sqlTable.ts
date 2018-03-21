/// <reference path="../../../../../typings/tsd.d.ts"/>

import sqlColumn = require("models/database/tasks/sql/sqlColumn");

class sqlTable {
    name = ko.observable<string>();
    columns = ko.observableArray<sqlColumn>([]);
}


export = sqlTable;

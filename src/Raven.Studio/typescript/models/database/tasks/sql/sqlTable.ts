/// <reference path="../../../../../typings/tsd.d.ts"/>

import sqlColumn = require("models/database/tasks/sql/sqlColumn");

class sqlTable {
    name = ko.observable<string>();
    customCollection = ko.observable<string>();
    primaryKeyColumns = ko.observableArray<sqlColumn>([]);
    columns = ko.observableArray<sqlColumn>([]);
    checked = ko.observable<boolean>(true);
    
    
    documentIdTemplate: KnockoutComputed<string>;
    
    constructor() {
        this.documentIdTemplate = ko.pureComputed(() => {
            const templetePart = this.primaryKeyColumns().map(x => '{' + x.name + '}').join("/");
            return this.customCollection() + "/" + templetePart;
        })
    }
}


export = sqlTable;
 

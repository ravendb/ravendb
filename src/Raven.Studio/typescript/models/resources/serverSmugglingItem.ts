/// <reference path="../../../typings/tsd.d.ts"/>

import database = require("models/resources/database");

class serverSmugglingItem {

    database: database;
    incremental = ko.observable<boolean>(true);

    constructor(database: database) {
        this.database = database;
    }
    /* TODO
    toDto(): serverSmugglingItemDto {
        return {
            Name: this.database.name,
            Incremental: this.incremental()
        }
    }*/
}

export = serverSmugglingItem;

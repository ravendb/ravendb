/// <reference path="../../../../typings/tsd.d.ts"/>

import resourceInfo = require("models/resources/info/resourceInfo");
import database = require("models/resources/database");

class databaseInfo extends resourceInfo {

    indexingDisabled = ko.observable<boolean>();
    rejectClientsEnabled = ko.observable<boolean>();

    constructor(dto: Raven.Client.Data.DatabaseInfo) {
        super(dto);
        this.indexingDisabled(dto.IndexingDisabled);
        this.rejectClientsEnabled(dto.RejectClientsEnabled);
    }

    get qualifier() {
        return "db";
    }

    get fullTypeName() {
        return "database";
    }

    get urlPrefix() {
        return "databases";
    }

    asResource(): database {
        return new database(this.name);
    }
}

export = databaseInfo;

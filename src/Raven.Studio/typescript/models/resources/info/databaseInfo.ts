/// <reference path="../../../../typings/tsd.d.ts"/>

import resourceInfo = require("models/resources/info/resourceInfo");
import database = require("models/resources/database");

class databaseInfo extends resourceInfo {

    rejectClients = ko.observable<boolean>();
    indexingStatus = ko.observable<Raven.Client.Data.Indexes.IndexRunningStatus>();
    indexingEnabled = ko.observable<boolean>();
    documentsCount = ko.observable<number>();
    indexesCount = ko.observable<number>();

    constructor(dto: Raven.Client.Data.DatabaseInfo) {
        super(dto);
        this.rejectClients(dto.RejectClients);
        this.indexingStatus(dto.IndexingStatus);
        this.indexingEnabled(dto.IndexingStatus === ("Running" as Raven.Client.Data.Indexes.IndexRunningStatus));
        this.documentsCount(dto.DocumentsCount);
        this.indexesCount(dto.IndexesCount);
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
        return new database(this.name, this.isAdmin(), this.disabled(), this.bundles());
    }
}

export = databaseInfo;

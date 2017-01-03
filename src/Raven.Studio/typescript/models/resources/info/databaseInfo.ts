/// <reference path="../../../../typings/tsd.d.ts"/>

import resourceInfo = require("models/resources/info/resourceInfo");
import database = require("models/resources/database");
import resourcesManager = require("common/shell/resourcesManager");

class databaseInfo extends resourceInfo {

    rejectClients = ko.observable<boolean>();
    indexingStatus = ko.observable<Raven.Client.Data.Indexes.IndexRunningStatus>();
    indexingEnabled = ko.observable<boolean>();
    documentsCount = ko.observable<number>();
    indexesCount = ko.observable<number>();

    constructor(dto: Raven.Client.Data.DatabaseInfo) {
        super(dto);

        this.update(dto);
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
        return resourcesManager.default.getDatabaseByName(this.name);
    }

    update(databaseInfo: Raven.Client.Data.DatabaseInfo): void {
        super.update(databaseInfo);
        this.rejectClients(databaseInfo.RejectClients);
        this.indexingStatus(databaseInfo.IndexingStatus);
        this.indexingEnabled(databaseInfo.IndexingStatus === "Running");
        this.documentsCount(databaseInfo.DocumentsCount);
        this.indexesCount(databaseInfo.IndexesCount);
    }
}

export = databaseInfo;

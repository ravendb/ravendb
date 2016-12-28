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
        this.indexingEnabled(dto.IndexingStatus === "Running");
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

    update(databaseInfo: Raven.Client.Data.DatabaseInfo): void {
        super.update(databaseInfo);
        this.rejectClients(databaseInfo.RejectClients);
        this.indexingStatus(databaseInfo.IndexingStatus);
        this.indexingEnabled(databaseInfo.IndexingStatus === "Running");
        this.documentsCount(databaseInfo.DocumentsCount);
        this.indexesCount(databaseInfo.IndexesCount);
    }

    static empty(resourceName: string): databaseInfo {
        let dto: Raven.Client.Data.DatabaseInfo = {
            Name: resourceName,
            Alerts: 0,
            BackupInfo: {
                FullBackupInterval: null,
                IncrementalBackupInterval: null,
                LastFullBackup: null,
                LastIncrementalBackup: null
            },
            Bundles: [],
            Disabled: false,
            Errors: 0,
            IsAdmin: false,
            TotalSize: { HumaneSize: "0 Bytes", SizeInBytes: 0 },
            UpTime: null,
            DocumentsCount: 0,
            IndexesCount: 0,
            IndexingStatus: 'Running' as Raven.Client.Data.Indexes.IndexRunningStatus,
            RejectClients: false
        };

        let dbInfo = new databaseInfo(dto);
        dbInfo.indexingEnabled(true);
        dbInfo.backupEnabled(false);
        return dbInfo;
    }
}

export = databaseInfo;

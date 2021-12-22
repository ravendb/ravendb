/// <reference path="../../../../typings/tsd.d.ts"/>

import generalUtils = require("common/generalUtils");
import moment = require("moment");

class restorePoint {
    dateTime: string;
    location: string;
    fileName: string;
    isSnapshotRestore: boolean;
    isIncremental: boolean;
    isEncrypted: boolean;
    filesToRestore: number;
    databaseName = ko.observable<string>();
    nodeTag: string;

    backupType: KnockoutComputed<string>;

    constructor(dto: Raven.Server.Documents.PeriodicBackup.Restore.RestorePoint) {
        const dateFormat = generalUtils.dateFormat;
        this.dateTime = moment(dto.DateTime).format(dateFormat);
        this.location = dto.Location;
        this.fileName = dto.FileName;
        this.isSnapshotRestore = dto.IsSnapshotRestore;
        this.isIncremental = dto.IsIncremental;
        this.isEncrypted = dto.IsEncrypted;
        this.filesToRestore = dto.FilesToRestore;
        this.databaseName(dto.DatabaseName);
        this.nodeTag = dto.NodeTag || "-";

        this.backupType = ko.pureComputed(() => {
            let backupType = "";
            if (this.isSnapshotRestore) {
                if (this.isIncremental) {
                    backupType = "Incremental ";
                }
                backupType += "Snapshot";
            } else if (this.isIncremental) {
                backupType = "Incremental";
            } else {
                backupType = "Full";
            }

            return backupType;
        });
    }
}

export = restorePoint;

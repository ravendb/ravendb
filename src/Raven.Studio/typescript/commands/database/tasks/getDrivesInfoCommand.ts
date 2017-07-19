import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getDrivesInfoCommand extends commandBase {
    
    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Documents.PeriodicBackup.DrivesInfo> {
        const url = endpoints.global.adminDatabases.adminPeriodicBackupDrivesInfo +
            this.urlEncodeArgs({ name: this.db.name });

        return this.query(url, null);
    }
}

export = getDrivesInfoCommand;

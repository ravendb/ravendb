import deleteDocumentCommand = require("commands/deleteDocumentCommand");
import commandBase = require("commands/commandBase");
import database = require("models/database");
import getDocumentWithMetadataCommand = require("commands/getDocumentWithMetadataCommand");
import appUrl = require("common/appUrl");

class monitorRestoreCommand extends commandBase {
    private db: database = appUrl.getSystemDatabase();

    constructor(private parentPromise: JQueryDeferred<any>, private updateRestoreStatus: (restoreStatusDto) => void) {
        super();
    }

    execute(): JQueryPromise<any> {
        new getDocumentWithMetadataCommand("Raven/Restore/Status", this.db)
            .execute()
            .fail((response: JQueryXHR) => {
                setTimeout(() => this.execute(), 1000);
            })
            .done((restoreStatus: restoreStatusDto)=> {
                var lastMessage = restoreStatus.Messages.last();
                var isRestoreFinished =
                    lastMessage.contains("The new database was created") ||
                    lastMessage.contains("Restore Canceled") ||
                    lastMessage.contains("A database name must be supplied if the restore location does not contain a valid Database.Document file") ||
                    lastMessage.contains("Cannot do an online restore for the <system> database") ||
                    lastMessage.contains("Restore ended but could not create the datebase document, in order to access the data create a database with the appropriate name");

                restoreStatus.IsRunning = !isRestoreFinished;
                this.updateRestoreStatus(restoreStatus);

                if (!isRestoreFinished) {
                    setTimeout(() => this.execute(), 1000);
                } else {
                    this.reportSuccess("Database was successfully restored!");
                    this.parentPromise.resolve();
                }
            });
        return this.parentPromise;
    }
}

export = monitorRestoreCommand;
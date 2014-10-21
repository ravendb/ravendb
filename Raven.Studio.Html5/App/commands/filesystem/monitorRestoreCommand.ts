import deleteDocumentCommand = require("commands/deleteDocumentCommand");
import commandBase = require("commands/commandBase");
import database = require("models/database");
import getDocumentWithMetadataCommand = require("commands/getDocumentWithMetadataCommand");

class monitorRestoreCommand extends commandBase {
    private db: database = new database("<system>");

    constructor(private parentPromise: JQueryDeferred<any>, private filesystemName: string, private updateRestoreStatus: (restoreStatusDto) => void) {
        super();
    }

    execute(): JQueryPromise<any> {
        new getDocumentWithMetadataCommand("Raven/FileSystem/Restore/Status/" + this.filesystemName, this.db)
            .execute()
            .fail((response: JQueryXHR) => {
                setTimeout(() => this.execute(), 1000);
            })
            .done((restoreStatus: restoreStatusDto)=> {
                var lastMessage = restoreStatus.Messages.last();
                var isRestoreFinished =
                    lastMessage.contains("The new filesystem was created") ||
                    lastMessage.contains("Restore Canceled") ||
                    lastMessage.contains("A filesystem name must be supplied if the restore location does not contain a valid") ||
                    lastMessage.contains("Restore ended but could not create the datebase document, in order to access the data create a database with the appropriate name");

                restoreStatus.IsRunning = !isRestoreFinished;
                this.updateRestoreStatus(restoreStatus);

                if (!isRestoreFinished) {
                    setTimeout(() => this.execute(), 1000);
                } else {
                    this.reportSuccess("Filesystem was successfully restored!");
                    this.parentPromise.resolve();
                }
            });
        return this.parentPromise;
    }
}

export = monitorRestoreCommand;
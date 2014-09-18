import deleteDocumentCommand = require("commands/deleteDocumentCommand");
import commandBase = require("commands/commandBase");
import database = require("models/database");
import getDocumentWithMetadataCommand = require("commands/getDocumentWithMetadataCommand");

class startRestoreCommand extends commandBase {
    private db: database = new database("<system>");

    constructor(private defrag: boolean, private restoreRequest: filesystemRestoreRequestDto, private updateRestoreStatus: (restoreStatusDto) => void) {
        super();
    }

    execute(): JQueryPromise<any> {
        var result = $.Deferred();

        new deleteDocumentCommand('Raven/FileSystem/Restore/Status/' + this.restoreRequest.FilesystemName, this.db)
            .execute()
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to delete restore status document!", response.responseText, response.statusText);
                result.reject();
            })
            .done(_=> {
            this.post('/admin/fs/restore?defrag=' + this.defrag, ko.toJSON(this.restoreRequest), null, { dataType: 'text' })
                .fail((response: JQueryXHR) => {
                    this.reportError("Failed to restore backup!", response.responseText, response.statusText);
                    this.logError(response, result);
                })
                .done(() => this.getRestoreStatus(result));
            });

        return result;
    }

    private logError(response: JQueryXHR, result: JQueryDeferred<any>) {
        var r = JSON.parse(response.responseText);
        var restoreStatus: restoreStatusDto = { Messages: [r.Error], IsRunning: false };
        this.updateRestoreStatus(restoreStatus);
        result.reject();
    }

    private getRestoreStatus(result: JQueryDeferred<any>) {
        new getDocumentWithMetadataCommand("Raven/FileSystem/Restore/Status/" + this.restoreRequest.FilesystemName, this.db)
            .execute()
            .fail((response: JQueryXHR) => {
                setTimeout(() => this.getRestoreStatus(result), 1000);
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
                    setTimeout(() => this.getRestoreStatus(result), 1000);
                } else {
                    this.reportSuccess("Filesystem was successfully restored!");
                    result.resolve();
                }
            });
    }
}

export = startRestoreCommand;
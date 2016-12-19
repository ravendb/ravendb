import deleteDocumentCommand = require("commands/database/documents/deleteDocumentCommand");
import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import monitorRestoreCommand = require("commands/filesystem/monitorRestoreCommand");
import appUrl = require("common/appUrl");

class startRestoreCommand extends commandBase {
    private db: database = appUrl.getSystemDatabase();

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
            this.post('/admin/fs-restore?defrag=' + this.defrag, ko.toJSON(this.restoreRequest), null, { dataType: 'text' })
                .fail((response: JQueryXHR) => {
                    this.reportError("Failed to restore backup!", response.responseText, response.statusText);
                    this.logError(response, result);
                    result.reject();
                })
                .done(() => new monitorRestoreCommand(result, this.restoreRequest.FilesystemName, this.updateRestoreStatus).execute());
            });

        return result;
    }

    private logError(response: JQueryXHR, result: JQueryDeferred<any>) {
        var r = JSON.parse(response.responseText);
        var restoreStatus: restoreStatusDto = { Messages: [r.Error], State: "Faulted" };
        this.updateRestoreStatus(restoreStatus);
        result.reject();
    }
}

export = startRestoreCommand;

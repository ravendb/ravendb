import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import appUrl = require("common/appUrl");
import deleteDocumentCommand = require("commands/database/documents/deleteDocumentCommand");
import monitorCompactCommand = require("commands/filesystem/monitorCompactCommand");

class startCompactCommand extends commandBase {

    private db: database = appUrl.getSystemDatabase();

    constructor(private fsToCompact: string, private updateCompactStatus: (compactStatusDto) => void) {
        super();
    }

    execute(): JQueryPromise<any> {

        var result = $.Deferred();

        new deleteDocumentCommand('Raven/Database/FileSystem/Status/' + this.fsToCompact, this.db)
            .execute()
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to delete compact status document!", response.responseText, response.statusText);
                result.reject();
            })
            .done(_=> {
                var url = '/admin/fs-compact' + this.urlEncodeArgs({ filesystem: this.fsToCompact });
                this.post(url, null, this.db)
                    .fail((response: JQueryXHR) => {
                        this.reportError("Failed to compact filesystem!", response.responseText, response.statusText);
                        this.logError(response, result);
                    })
                    .done(() => new monitorCompactCommand(result, this.fsToCompact, this.updateCompactStatus).execute());
            });

        return result;
    }


    private logError(response: JQueryXHR, result: JQueryDeferred<any>) {
        var r = JSON.parse(response.responseText);
        var compactStatus: compactStatusDto = { Messages: [r.Error], LastProgressMessage:"", State: "Faulted" };
        this.updateCompactStatus(compactStatus);
        result.reject();
    }
}

export = startCompactCommand;

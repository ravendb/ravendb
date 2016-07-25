import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import appUrl = require("common/appUrl");
import deleteDocumentCommand = require("commands/database/documents/deleteDocumentCommand");
import monitorCompactCommand = require("commands/maintenance/monitorCompactCommand");

class startCompactCommand extends commandBase {

    constructor(private dbToCompact: string, private updateCompactStatus: (status: compactStatusDto) => void) {
        super();
    }

    execute(): JQueryPromise<any> {

        var result = $.Deferred();

        new deleteDocumentCommand('Raven/Database/Compact/Status/' + this.dbToCompact, null)
            .execute()
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to delete compact status document!", response.responseText, response.statusText);
                result.reject();
            })
            .done(_=> {
                var url = '/admin/compact' + this.urlEncodeArgs({ database: this.dbToCompact });
                this.post(url, null, null)
                    .fail((response: JQueryXHR) => {
                        this.reportError("Failed to compact database!", response.responseText, response.statusText);
                        this.logError(response, result);
                    })
                    .done(() => new monitorCompactCommand(result, this.dbToCompact, this.updateCompactStatus).execute());
            });

        return result;
    }

    private logError(response: JQueryXHR, result: JQueryDeferred<any>) {
        var r = JSON.parse(response.responseText);
        var compactStatus: compactStatusDto = { Messages: [r.Error], LastProgressMessage: "", State: "Faulted" };
        this.updateCompactStatus(compactStatus);
        result.reject();
    }
}

export = startCompactCommand;

import commandBase = require("commands/commandBase");
import database = require("models/database");
import document = require("models/document");

class saveGlobalSettingsCommand extends commandBase {

    constructor(private db: database, private document: document) {
        super();

        if (!db) {
            throw new Error("Must specify database");
        }
    }

    execute(): JQueryPromise<any> {
        this.reportInfo("Saving Global Settings...");

        var jQueryOptions: JQueryAjaxSettings = {
        };

        if (this.document.__metadata.etag) {
            jQueryOptions.headers = {
                'If-None-Match': this.document.__metadata.etag
            }
        }

        var args = JSON.stringify(this.document.toDto());
        var url = "/configuration/global/settings";
        var saveTask = this.put(url, args, null, jQueryOptions);
        saveTask.done(() => this.reportSuccess("Global Settings were successfully saved!"));
        saveTask.fail((response: JQueryXHR) => this.reportError("Failed to save global settings!", response.responseText, response.statusText));
        return saveTask;
    }
}

export = saveGlobalSettingsCommand;
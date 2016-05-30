import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import document = require("models/database/documents/document");

class saveDatabaseSettingsCommand extends commandBase {

    constructor(private db: database, private document: document) {
        super();

        if (!db) {
            throw new Error("Must specify database");
        }
    }

    execute(): JQueryPromise<databaseDocumentSaveDto> {
        this.reportInfo("Saving Database Settings for '" + this.db.name + "'...");

        var jQueryOptions: JQueryAjaxSettings = {
            headers: {
                'If-None-Match': this.document.__metadata.etag,
                'Raven-Temp-Allow-Bundles-Change': this.document.__metadata['Raven-Temp-Allow-Bundles-Change']
            }
        };

        var args = JSON.stringify(this.document.toDto());
        var url = "/admin/databases/" + this.db.name;
        var saveTask = this.put(url, args, null, jQueryOptions);

        saveTask.done(() => this.reportSuccess("Database Settings of '" + this.db.name + "' were successfully saved!"));
        saveTask.fail((response: JQueryXHR) => this.reportError("Failed to save Database Settings!", response.responseText, response.statusText));
        return saveTask;
    }

}

export = saveDatabaseSettingsCommand;

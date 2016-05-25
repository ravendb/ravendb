import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import document = require("models/database/documents/document");
import shell = require("viewmodels/shell");
import appUrl = require("common/appUrl");
import getDocumentWithMetadataCommand = require("commands/database/documents/getDocumentWithMetadataCommand");

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

        var savedAndAppliedTask: JQueryPromise<databaseDocumentSaveDto>;

        if (shell.clusterMode()) {
            // in case of cluster mode we don't get etag and key
            // pool for document until etag will be changed
            jQueryOptions.dataType = undefined;
            var saveTask = this.put(url, args, null, jQueryOptions);

            var changesAppliedTask = $.Deferred<databaseDocumentSaveDto>();
            savedAndAppliedTask = changesAppliedTask;
            saveTask.fail(() => changesAppliedTask.reject());
            saveTask.done(() => {
                var documentKey = "Raven/Databases/" + this.db.name;
                new getDocumentWithMetadataCommand(documentKey, appUrl.getSystemDatabase())
                    .execute()
                    .fail((request, status, error) => changesAppliedTask.reject(request, status, error))
                    .done((result: any) => {
                        var documentEtag = result.__metadata.etag;
                        changesAppliedTask.resolve({
                            ETag: documentEtag,
                            Key: documentKey
                        });
                    });
            });
        } else {
            savedAndAppliedTask = this.put(url, args, null, jQueryOptions);
        }
        

        savedAndAppliedTask.done(() => this.reportSuccess("Database Settings of '" + this.db.name + "' were successfully saved!"));
        savedAndAppliedTask.fail((response: JQueryXHR) => this.reportError("Failed to save Database Settings!", response.responseText, response.statusText));
        return savedAndAppliedTask;
    }

}

export = saveDatabaseSettingsCommand;

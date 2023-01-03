import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import document = require("models/database/documents/document");
import endpoints = require("endpoints");

class getDocumentsWithMetadataCommand extends commandBase {

    constructor(private ids: string[], private db: database) {
        super();
    }

    execute(): JQueryPromise<document[]> {
        const documentResult = $.Deferred<document[]>();

        const payload = {
            Ids: this.ids
        };
        this.post<queryResultDto<documentDto>>(endpoints.databases.document.docs, JSON.stringify(payload), this.db)
            .fail((xhr: JQueryXHR) => {
                if (xhr.status === 404 && this.ids.length === 1) {
                    documentResult.resolve([null]);
                } else {
                    documentResult.reject(xhr);
                }
            })
            .done((queryResult: queryResultDto<documentDto>) => {
                documentResult.resolve(queryResult.Results.map(x => new document(x)));
            });

        return documentResult;
    }
 }

 export = getDocumentsWithMetadataCommand;

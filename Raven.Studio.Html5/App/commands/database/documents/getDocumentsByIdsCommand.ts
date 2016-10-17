import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import document = require("models/database/documents/document");

class getDocumentsByIdsCommand extends commandBase {

    constructor(private ids: string[], private db: database) {
        super();
    }

    execute(): JQueryPromise<Array<document>> {
        var documentResult = $.Deferred();
        var postResult = this.post("/queries", JSON.stringify(this.ids), this.db);
        postResult.fail(xhr => documentResult.fail(xhr));
        postResult.done((queryResult: queryResultDto) => {
            if (queryResult.Results.length !== this.ids.length) {
                documentResult.reject("Unable to find documents with IDs: " + this.ids.join(", "));
            } else {
                documentResult.resolve(queryResult.Results.map(x => new document(x)));
            }
        });

        return documentResult;
    }
 }

export = getDocumentsByIdsCommand;

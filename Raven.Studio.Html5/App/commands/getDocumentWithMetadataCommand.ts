import commandBase = require("commands/commandBase");
import database = require("models/database");
import document = require("models/document");

class getDocumentWithMetadataCommand extends commandBase {

    constructor(private id: string, private db: database) {
        super();

        if (!id) {
            throw new Error("Must specify ID");
        }

        if (!db) {
            throw new Error("Must specify database");
        }
    }

    execute(): JQueryPromise<document> {
        // Executing /queries will return the doc with the metadata. 
        // We can do a GET call to /docs/[id], however, it returns the metadata only as headers, 
        // which can have some issues when querying via CORS.
        var documentResult = $.Deferred();
        var postResult = this.post("/queries", JSON.stringify([this.id]), this.db);
        postResult.fail(xhr => documentResult.fail(xhr));
        postResult.done((queryResult: queryResultDto) => {
            if (queryResult.Results.length === 0) {
                documentResult.reject("Unable to find document with ID " + this.id);
            } else {
                documentResult.resolve(new document(queryResult.Results[0]));
            }
        });

        return documentResult;
    }

 }

 export = getDocumentWithMetadataCommand;
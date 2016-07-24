import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import document = require("models/database/documents/document");

class getDocumentWithMetadataCommand extends commandBase {

    shouldResolveNotFoundAsNull: boolean;

    constructor(private id: string, private db: database, shouldResolveNotFoundAsNull?: boolean) {
        super();

        if (!id) {
            throw new Error("Must specify ID");
        }

        this.shouldResolveNotFoundAsNull = shouldResolveNotFoundAsNull || false;
    }

    execute(): JQueryPromise<any> {

        var documentResult = $.Deferred();
        var postResult = this.post("/docs", JSON.stringify([this.id]), this.db);
        postResult.fail(xhr => documentResult.fail(xhr));
        postResult.done((queryResult: queryResultDto) => {
            if (queryResult.Results.length === 0) {
                if (this.shouldResolveNotFoundAsNull) {
                    documentResult.resolve(null);
                } else {
                    documentResult.reject("Unable to find document with ID " + this.id);
                }
            } else {
                documentResult.resolve(new document(queryResult.Results[0]));
            }
        });

        return documentResult;
    }
 }

 export = getDocumentWithMetadataCommand;

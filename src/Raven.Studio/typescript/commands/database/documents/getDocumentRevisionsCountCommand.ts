import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getDocumentRevisionsCountCommand extends commandBase {

    constructor(private id: string, private db: database) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Documents.Handlers.DocumentRevisionsCount> {
        const args = {
            id: this.id
        };

        const url = endpoints.databases.revisions.revisionsCount + this.urlEncodeArgs(args);

        return this.query<Raven.Server.Documents.Handlers.DocumentRevisionsCount>(url, null, this.db)
            .fail((result: JQueryXHR) => this.reportError("Failed to get revisions count", result.responseText, result.statusText));
    }
 }

export = getDocumentRevisionsCountCommand;

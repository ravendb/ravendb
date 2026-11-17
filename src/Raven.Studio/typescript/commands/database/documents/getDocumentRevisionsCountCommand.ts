import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getDocumentRevisionsCountCommand extends commandBase {

    constructor(private id: string, private db: database | string, private reportFailure = true) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Documents.Session.Operations.GetRevisionsCountOperation.DocumentRevisionsCount> {
        const args = {
            id: this.id
        };

        const url = endpoints.databases.revisions.revisionsCount + this.urlEncodeArgs(args);

        const task = this.query<Raven.Client.Documents.Session.Operations.GetRevisionsCountOperation.DocumentRevisionsCount>(url, null, this.db);

        if (this.reportFailure) {
            task.fail((result: JQueryXHR) => this.reportError("Failed to get revisions count", result.responseText, result.statusText));
        }

        return task;
    }
 }

export = getDocumentRevisionsCountCommand;

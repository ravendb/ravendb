import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class deleteRevisionsForDocumentsCommand extends commandBase {

    constructor(private ids: Array<string>, private db: database) {
        super();
    }

    execute(): JQueryPromise<void> {
        const payload: Raven.Server.Documents.Revisions.DeleteRevisionsOperation.Parameters = {
            DocumentIds: this.ids
        };

        const url = endpoints.databases.adminRevisions.adminRevisions;

        return this.del<void>(url, JSON.stringify(payload), this.db, { dataType: undefined });
    }
 }

export = deleteRevisionsForDocumentsCommand;

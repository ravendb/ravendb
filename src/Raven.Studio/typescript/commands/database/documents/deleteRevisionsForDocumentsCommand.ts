import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class deleteRevisionsForDocumentsCommand extends commandBase {

    constructor(private ids: Array<string>, private db: database) {
        super();
    }

    execute(): JQueryPromise<void> {
        const payload = {
            DocumentIds: this.ids
        } as Raven.Server.Documents.Handlers.Admin.AdminRevisionsHandler.Parameters;

        const url = endpoints.databases.adminRevisions.adminRevisions;

        return this.del<void>(url, JSON.stringify(payload), this.db, { dataType: undefined });
    }
 }

export = deleteRevisionsForDocumentsCommand;

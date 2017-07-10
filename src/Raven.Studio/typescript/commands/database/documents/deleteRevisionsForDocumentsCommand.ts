import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class deleteRevisionsForDocumentsCommand extends commandBase {

    constructor(private ids: Array<string>, private db: database) {
        super();
    }

    execute(): JQueryPromise<void> {
        const args = {
            id: this.ids
        }

        const url = endpoints.databases.adminRevisions.adminRevisions + this.urlEncodeArgs(args);

        return this.del<void>(url, null, this.db, { dataType: undefined });
    }
 }

export = deleteRevisionsForDocumentsCommand;

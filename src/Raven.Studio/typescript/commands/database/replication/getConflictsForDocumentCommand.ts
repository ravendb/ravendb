import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getConflictsForDocumentCommand extends commandBase {

    constructor(private ownerDb: database, private documentId: string) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Documents.Commands.GetConflictsResult> {
        const args = {
            docId: this.documentId
        };
        const url = endpoints.databases.replication.replicationConflicts + this.urlEncodeArgs(args);

        return this.query<Raven.Client.Documents.Commands.GetConflictsResult>(url, null, this.ownerDb);
    }
}

export = getConflictsForDocumentCommand;

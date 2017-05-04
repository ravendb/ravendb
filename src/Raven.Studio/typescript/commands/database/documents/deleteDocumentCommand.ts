import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class deleteDocumentCommand extends commandBase {

    constructor(private docId: string, private db: database) {
        super();
    }

    execute(): JQueryPromise<void> {
        const args = {
            id: this.docId
        };
        const url = endpoints.databases.document.docs + this.urlEncodeArgs(args);
        return this.del<void>(url, null, this.db);
    }
}

export = deleteDocumentCommand;

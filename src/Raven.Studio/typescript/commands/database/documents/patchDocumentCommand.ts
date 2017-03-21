import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class patchDocumentCommand extends commandBase {
    constructor(private documentId: string, private script: string, private test: boolean, private db: database) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Documents.Operations.PatchResult> {

        const args = {
            id: this.documentId,
            test: this.test
        };

        const url = endpoints.databases.document.docs + this.urlEncodeArgs(args);

        const payload = {
            Patch: {
                Script: this.script
            } as Raven.Server.Documents.Patch.PatchRequest
        }

        return this.patch(url, JSON.stringify(payload), this.db);
    }
}

export = patchDocumentCommand; 

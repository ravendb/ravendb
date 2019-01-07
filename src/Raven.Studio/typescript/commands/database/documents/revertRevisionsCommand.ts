import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class revertRevisionsCommand extends commandBase {

    constructor(private payload: Raven.Server.Documents.Revisions.RevertRevisionsRequest, private db: database) {
        super();
    }

    execute(): JQueryPromise<operationIdDto> {
        const url = endpoints.databases.revisions.revisionsRevert;

        return this.post(url, JSON.stringify(this.payload), this.db)
            .fail((response: JQueryXHR) => this.reportError("Failed to revert document revisions", response.responseText));
    }
 }

export = revertRevisionsCommand;

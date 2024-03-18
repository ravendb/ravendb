import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import RevertRevisionsRequest = Raven.Server.Documents.Revisions.RevertRevisionsRequest;

class revertRevisionsCommand extends commandBase {
    private readonly payload: RevertRevisionsRequest;
    private readonly db: database | string;

    constructor(payload: RevertRevisionsRequest, db: database | string) {
        super();
        this.db = db;
        this.payload = payload;
    }

    execute(): JQueryPromise<operationIdDto> {
        const url = endpoints.databases.revisions.revisionsRevert;

        return this.post(url, JSON.stringify(this.payload), this.db).fail((response: JQueryXHR) =>
            this.reportError("Failed to revert document revisions", response.responseText)
        );
    }
}

export = revertRevisionsCommand;

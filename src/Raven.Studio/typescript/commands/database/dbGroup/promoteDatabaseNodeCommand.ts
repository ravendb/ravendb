import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

type PromoteImmediatelyResultDto = Pick<
    Raven.Client.ServerWide.Operations.DatabasePutResult,
    "Name" | "RaftCommandIndex"
>;

class promoteDatabaseNodeCommand extends commandBase {
    constructor(private databaseName: string, private nodeTag: string) {
        super();
    }

    execute(): JQueryPromise<PromoteImmediatelyResultDto> {
        const args = {
            name: this.databaseName,
            node: this.nodeTag,
        };

        const url = endpoints.global.adminDatabases.adminDatabasesPromote + this.urlEncodeArgs(args);

        return this.post<PromoteImmediatelyResultDto>(url, null, null).fail((response: JQueryXHR) =>
            this.reportError("Failed to promote node " + this.nodeTag, response.responseText, response.statusText)
        );
    }
}

export = promoteDatabaseNodeCommand;

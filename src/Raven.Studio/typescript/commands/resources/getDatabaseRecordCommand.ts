import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import document = require("models/database/documents/document");
import endpoints = require("endpoints");

class getDatabaseRecordCommand extends commandBase {

    private readonly databaseName: string;
    private readonly reportRefreshProgress: boolean;

    constructor(db: database | string, reportRefreshProgress = false) {
        super();
        this.databaseName = (typeof db === "string" ? db : db.name);
        this.reportRefreshProgress = reportRefreshProgress;
    }

    execute(): JQueryPromise<document> {
        const resultsSelector = (queryResult: queryResultDto<documentDto>) => new document(queryResult);
        const args = {
            name: this.databaseName
        };
        const url = endpoints.global.adminDatabases.adminDatabases + this.urlEncodeArgs(args);

        const getTask = this.query(url, null, null, resultsSelector);

        if (this.reportRefreshProgress) {
            getTask.done(() => this.reportSuccess("Database Record of '" + this.databaseName + "' was successfully refreshed!"));
            getTask.fail((response: JQueryXHR) => this.reportError("Failed to refresh Database Record!", response.responseText, response.statusText));
        }
        return getTask;
    }
}

export = getDatabaseRecordCommand;

import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class retryBatchEtlCommand extends commandBase {
    constructor(private db: database | string, private etlProcessName: string, private location: databaseLocationSpecifier) {
        super();
    }

    execute(): JQueryPromise<void> {
        const args = { ...this.location, name: this.etlProcessName };
        
        const url = endpoints.databases.etl.etlRetryBatch + this.urlEncodeArgs(args);

        return this.post<void>(url, null, this.db, { dataType: "text" })
            .done(() => this.reportSuccess("ETL batch retry was triggered."))
            .fail((response: JQueryXHR) => this.reportError("Failed to retry ETL batch.", response.responseText, response.statusText));
    }
}

export = retryBatchEtlCommand;


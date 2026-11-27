import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class etlProgressCommand extends commandBase {

    constructor(private readonly db: database | string, private readonly location: databaseLocationSpecifier, private readonly reportFailure = true, private readonly taskName: string = null, private readonly exact = false) {
        super();
    }

    execute(): JQueryPromise<resultsDto<Raven.Server.Documents.ETL.Stats.EtlTaskProgress>> {
        const args = {
            ...this.location,
            name: this.taskName ?? undefined,
            exact: this.exact ? true : undefined,
        };
        const url = endpoints.databases.etl.etlProgress + this.urlEncodeArgs(args);

        return this.query<resultsDto<Raven.Server.Documents.ETL.Stats.EtlTaskProgress>>(url, null, this.db)
            .fail((response: JQueryXHR) => {
                if (this.reportFailure) {
                    this.reportError(`Failed to fetch Etl progress`, response.responseText);    
                }
            });
    }
}

export = etlProgressCommand; 

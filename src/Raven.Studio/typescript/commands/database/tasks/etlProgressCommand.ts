import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class etlProgressCommand extends commandBase {

    constructor(private db: database, private location: databaseLocationSpecifier, private reportFailure: boolean = true) {
        super();
    }

    execute(): JQueryPromise<resultsDto<Raven.Server.Documents.ETL.Stats.EtlTaskProgress>> {
        const url = endpoints.databases.etl.etlProgress;
        const args = this.location;

        return this.query<resultsDto<Raven.Server.Documents.ETL.Stats.EtlTaskProgress>>(url, args, this.db)
            .fail((response: JQueryXHR) => {
                if (this.reportFailure) {
                    this.reportError(`Failed to fetch Etl progress`, response.responseText);    
                }
            });
    }
}

export = etlProgressCommand; 

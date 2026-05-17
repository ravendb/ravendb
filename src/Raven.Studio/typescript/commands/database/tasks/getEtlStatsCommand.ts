import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import EtlTaskStats = Raven.Server.Documents.ETL.Stats.EtlTaskStats;

interface EtlStatsArgs extends databaseLocationSpecifier {
    name?: string[];
}

class getEtlStatsCommand extends commandBase {
    constructor(private db: database | string, private location: databaseLocationSpecifier, private names: string[] = []) {
        super();
    }

    execute(): JQueryPromise<EtlTaskStats[]> {
        const args: EtlStatsArgs = {...this.location};

        if (this.names.length > 0) {
            args.name = this.names;
        }
        const url = endpoints.databases.etl.etlStats + this.urlEncodeArgs(args);

        return this.query<EtlTaskStats[]>(url, null, this.db, (results) => results.Results)
            .fail((response: JQueryXHR) => this.reportError(`Failed to fetch ETL stats`, response.responseText, response.statusText));
    }
}

export = getEtlStatsCommand;

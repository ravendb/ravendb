import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getSqlReplicationStatsCommand extends commandBase {
    constructor(private ownerDb: database, private sqlReplicationName: string) {
        super();

        if (!this.ownerDb) {
            throw new Error("Must specify a database.");
        }
    }

    execute(): JQueryPromise<Raven.Server.Documents.ETL.EtlStatistics> { // TODO warapped by 'Results'
        const args = {
            name: this.sqlReplicationName
        };

        const url = endpoints.databases.etl.etlStats;

        return this.query(url, args, this.ownerDb);
    }
}

export = getSqlReplicationStatsCommand;

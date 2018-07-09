import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class simulateSqlReplicationCommand extends commandBase {
    constructor(private db: database, private payload: Raven.Server.Documents.ETL.Providers.SQL.RelationalWriters.SimulateSqlEtl) {
        super();
    }  

    execute(): JQueryPromise<Raven.Server.Documents.ETL.Providers.SQL.Simulation.SqlEtlSimulationResult> {
        const url = endpoints.databases.sqlEtl.adminEtlSqlSimulate;

        return this.post<Raven.Server.Documents.ETL.Providers.SQL.Simulation.SqlEtlSimulationResult>(url, JSON.stringify(this.payload), this.db)
            .fail((response: JQueryXHR) => {                         
                this.reportError(`Failed to simulate SQL replication`, response.responseText, response.statusText);
            });
    }
}

export = simulateSqlReplicationCommand; 


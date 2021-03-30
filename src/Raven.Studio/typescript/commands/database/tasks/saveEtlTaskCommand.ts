import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class saveEtlTaskCommand<T extends Raven.Client.Documents.Operations.ETL.RavenEtlConfiguration | 
                                   Raven.Client.Documents.Operations.ETL.SQL.SqlEtlConfiguration |
                                   Raven.Client.Documents.Operations.ETL.OLAP.OlapEtlConfiguration> extends commandBase {
    
    private constructor(private db: database, private payload: T, private scriptsToReset?: string[]) {
        super();
    }  

    execute(): JQueryPromise<Raven.Client.Documents.Operations.OngoingTasks.ModifyOngoingTaskResult> {
        return this.updateEtl()
            .fail((response: JQueryXHR) => {
                this.reportError(`Failed to save ${this.payload.EtlType.toUpperCase()} ETL task`, response.responseText, response.statusText);
            })
            .done(() => {
                this.reportSuccess(`Saved ${this.payload.EtlType.toUpperCase()} ETL task`); 
            });
    }

    private updateEtl(): JQueryPromise<Raven.Client.Documents.Operations.OngoingTasks.ModifyOngoingTaskResult> {
        
        const args = {
            id : this.payload.TaskId || undefined,
            reset: this.scriptsToReset || undefined
        };
        
        const url = endpoints.databases.ongoingTasks.adminEtl + this.urlEncodeArgs(args);
        
        return this.put(url, JSON.stringify(this.payload), this.db);
    }

    static forRavenEtl(db: database, payload: Raven.Client.Documents.Operations.ETL.RavenEtlConfiguration, scriptsToReset?: string[]) {
        return new saveEtlTaskCommand<Raven.Client.Documents.Operations.ETL.RavenEtlConfiguration>(db, payload, scriptsToReset);
    }

    static forSqlEtl(db: database, payload: Raven.Client.Documents.Operations.ETL.SQL.SqlEtlConfiguration, scriptsToReset?: string[]) {
        return new saveEtlTaskCommand<Raven.Client.Documents.Operations.ETL.SQL.SqlEtlConfiguration>(db, payload, scriptsToReset);
    }

    static forOlapEtl(db: database, payload: Raven.Client.Documents.Operations.ETL.OLAP.OlapEtlConfiguration, scriptsToReset?: string[]) {
        return new saveEtlTaskCommand<Raven.Client.Documents.Operations.ETL.OLAP.OlapEtlConfiguration>(db, payload, scriptsToReset);
    }
}

export = saveEtlTaskCommand;

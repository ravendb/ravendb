import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class saveEtlTaskCommand<T extends Raven.Client.ServerWide.ETL.RavenEtlConfiguration | Raven.Client.ServerWide.ETL.SqlEtlConfiguration> extends commandBase {
    private constructor(private db: database, private payload: T, private scriptsToReset?: string[]) {
        super();
    }  

    execute(): JQueryPromise<Raven.Client.ServerWide.Operations.ModifyOngoingTaskResult> {
        return this.updateEtl()
            .fail((response: JQueryXHR) => {                         
                this.reportError(`Failed to save ${this.payload.EtlType.toUpperCase()} ETL task`, response.responseText, response.statusText);
            })
            .done(() => {
                this.reportSuccess(`Saved ${this.payload.EtlType.toUpperCase()} ETL task`); 
            });
    }

    private updateEtl(): JQueryPromise<Raven.Client.ServerWide.Operations.ModifyOngoingTaskResult> {        
        
        let scriptsToResetQueryString :string = "";
        for (let i = 0; i < this.scriptsToReset.length; i++) {
             scriptsToResetQueryString += `&reset=${encodeURIComponent(this.scriptsToReset[i])}`;   
        }
        
        const args = this.payload.TaskId ? { name: this.db.name, id: this.payload.TaskId } : { name: this.db.name };
        const url = endpoints.global.adminDatabases.adminEtl + this.urlEncodeArgs(args) + scriptsToResetQueryString;

        return this.put(url, JSON.stringify(this.payload));
    }

    static forRavenEtl(db: database, payload: Raven.Client.ServerWide.ETL.RavenEtlConfiguration, scriptsToReset?: string[]) {
        return new saveEtlTaskCommand<Raven.Client.ServerWide.ETL.RavenEtlConfiguration>(db, payload, scriptsToReset);
    }

    static forSqlEtl(db: database, payload: Raven.Client.ServerWide.ETL.SqlEtlConfiguration) {
        return new saveEtlTaskCommand<Raven.Client.ServerWide.ETL.SqlEtlConfiguration>(db, payload);
    }    
}

export = saveEtlTaskCommand; 


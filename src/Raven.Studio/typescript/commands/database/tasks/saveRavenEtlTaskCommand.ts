import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class saveRavenEtlTaskCommand extends commandBase {

    constructor(private db: database, private taskId: number, private payload: Raven.Client.ServerWide.ETL.RavenEtlConfiguration) {
        super();
    }
 
    execute(): JQueryPromise<Raven.Client.ServerWide.Operations.ModifyOngoingTaskResult> {
        return this.updateRavenEtl()
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to save RavenDB ETL task", response.responseText, response.statusText);
            })
            .done(() => {
                this.reportSuccess(`Saved RavenDB ETL task`);
            });
    }
    private updateRavenEtl(): JQueryPromise<Raven.Client.ServerWide.Operations.ModifyOngoingTaskResult> {
        const args = this.taskId ? { name: this.db.name, id: this.taskId } : { name: this.db.name };

        const url = endpoints.global.adminDatabases.adminEtl + this.urlEncodeArgs(args);

        return this.put(url, JSON.stringify(this.payload));
    }
}

export = saveRavenEtlTaskCommand; 


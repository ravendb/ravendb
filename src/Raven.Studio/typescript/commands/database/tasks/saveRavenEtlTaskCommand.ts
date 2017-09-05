import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class saveRavenEtlTaskCommand extends commandBase {

    constructor(private db: database, private taskId: number, private ravenEtlSettings: ravenEtlDataFromUI) {
        super();
    }
 
    execute(): JQueryPromise<Raven.Client.ServerWide.Operations.ModifyOngoingTaskResult> {
        return this.updateRavenEtl()
            .fail((response: JQueryXHR) => {
                if (this.taskId) {
                   this.reportError("Failed to save RavenDB ETL task", response.responseText, response.statusText);
                } else {
                    this.reportError("Failed to create RavenDB ETL task", response.responseText, response.statusText);
                }
            })
            .done(() => {
                if (this.taskId) {
                    this.reportSuccess(`Updated RavenDB ETL task`);
                } else {
                    this.reportSuccess(`Created RavenDB ETL task`);
                }
            });
    }

    private updateRavenEtl(): JQueryPromise<Raven.Client.ServerWide.Operations.ModifyOngoingTaskResult> {

        const url = endpoints.global.adminDatabases.adminEtl;

        const addRavenEtlTask = $.Deferred<Raven.Client.ServerWide.Operations.ModifyOngoingTaskResult>();

        // TODO.......

        return addRavenEtlTask;
    }
}

export = saveRavenEtlTaskCommand; 


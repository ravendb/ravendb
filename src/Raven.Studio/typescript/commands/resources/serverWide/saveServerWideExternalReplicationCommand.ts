import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class saveServerWideExternalReplicationCommand extends commandBase {
    constructor(private configuration: Raven.Client.ServerWide.Operations.OngoingTasks.ServerWideExternalReplication) {
        super();
    } 
    
    execute(): JQueryPromise<Raven.Client.ServerWide.Operations.Configuration.PutServerWideBackupConfigurationResponse> {
        const url = endpoints.global.adminServerWide.adminConfigurationServerWideExternalReplication;
        
        const isNewTask = this.configuration.TaskId === 0;
        
        return this.put<Raven.Client.ServerWide.Operations.OngoingTasks.ServerWideExternalReplicationResponse>(url, JSON.stringify(this.configuration))
            .done((results: Raven.Client.ServerWide.Operations.OngoingTasks.ServerWideExternalReplicationResponse) => {
                const taskTypeText = isNewTask ? "created" : "updated";
                this.reportSuccess(`The server-wide external replication task was ${taskTypeText} successfully`);
            })
            .fail(response => this.reportError(`Failed to save the server-wide external replication task: ${this.configuration.Name}`, response.responseText, response.statusText));
    }
}

export = saveServerWideExternalReplicationCommand; 


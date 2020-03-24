import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class saveServerWideBackupCommand extends commandBase {
    constructor(private configuration: Raven.Client.ServerWide.Operations.Configuration.ServerWideBackupConfiguration) {
        super();
    } 
    
    execute(): JQueryPromise<Raven.Client.ServerWide.Operations.Configuration.PutServerWideBackupConfigurationResponse> {
        const url = endpoints.global.adminServerWide.adminConfigurationServerWideBackup;
        const isNewTask = this.configuration.TaskId === 0;
        
        return this.put<Raven.Client.ServerWide.Operations.Configuration.PutServerWideBackupConfigurationResponse>(url, JSON.stringify(this.configuration))
            .done((results: Raven.Client.ServerWide.Operations.Configuration.PutServerWideBackupConfigurationResponse) => {
                const taskTypeText = isNewTask ? "created" : "updated";
                this.reportSuccess(`Succefully ${taskTypeText} Server-wide backup configuration`);
            })
            .fail(response => this.reportError(`Failed to save Server-Wide Backup: ${this.configuration.Name}`, response.responseText, response.statusText));
    }
}

export = saveServerWideBackupCommand; 


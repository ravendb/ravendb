import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class toggleServerWideTaskCommand extends commandBase {

    constructor(private type: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskType, private name: string, private disable: boolean) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.ServerWide.Operations.Configuration.PutServerWideBackupConfigurationResponse> {
        const args = {
            type: this.type,
            name: this.name,
            disable: this.disable
        };
        
        const url = endpoints.global.adminServerWide.adminConfigurationServerWideState;

        const operationText = this.disable ? "disable" : "enable";
     
        return this.post(url + this.urlEncodeArgs(args), null)
            .done(() => this.reportSuccess(`Successfully ${operationText}d server-wide backup task ${this.name}`))
            .fail((response: JQueryXHR) => this.reportError(`Failed to ${operationText} ${this.name} server-wide backup task. `, response.responseText));
    }
}

export = toggleServerWideTaskCommand; 

import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class deleteServerWideTaskCommand extends commandBase {
    constructor(private type: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskType, private name: string) {
        super();
    }
    
    execute(): JQueryPromise<Raven.Client.ServerWide.Operations.Configuration.ServerWideTaskResponse> {
        const args = {
            type: this.type,
            name: this.name
        };
        const url = endpoints.global.adminServerWide.adminConfigurationServerWideTask + this.urlEncodeArgs(args);

        return this.del<Raven.Client.ServerWide.Operations.Configuration.PutServerWideBackupConfigurationResponse>(url, null)
            .done(() => this.reportSuccess(`Successfully deleted Server-Wide ${this.type} task: ${this.name}`))
            .fail((response: JQueryXHR) => this.reportError(`Failed to delete Server-Wide ${this.type} task: ${this.name}`, response.responseText));
    }
}

export = deleteServerWideTaskCommand;

import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class toggleServerWideBackupCommand extends commandBase {

    constructor(private taskName: string, private disable: boolean) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.ServerWide.Operations.Configuration.PutServerWideBackupConfigurationResponse> {
        const args = { taskName: this.taskName,  disable: this.disable };
        
        const url = endpoints.global.adminServerWideBackup.adminConfigurationServerWideBackupState;

        const operationText = this.disable ? "disable" : "enable";
     
        return this.post(url + this.urlEncodeArgs(args), null)
            .done(() => this.reportSuccess(`Successfully ${operationText}d server-wide backup task ${this.taskName}`))
            .fail((response: JQueryXHR) => this.reportError(`Failed to ${operationText} ${this.taskName} server-wide backup task. `, response.responseText));
    }
}

export = toggleServerWideBackupCommand; 

import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class setLicenseLimitsCommand extends commandBase {

    constructor(private nodeTag: string, private newAssignedCores: number) {
        super();
    }

    execute(): JQueryPromise<void> {
        const args = {
            nodeTag: this.nodeTag,
            newAssignedCores: this.newAssignedCores
        }

        const url = endpoints.global.rachisAdmin.adminLicenseSetLimit + this.urlEncodeArgs(args);

        return this.post(url, null, null, { dataType: undefined })
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to set new license limits", response.responseText, response.statusText);
            })
            .done(() => this.reportSuccess("Succefully set the new license limit"));
    }
}

export = setLicenseLimitsCommand; 

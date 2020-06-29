import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class setLicenseLimitsCommand extends commandBase {

    constructor(private nodeTag: string, private maxUtilizedCores: number | null) {
        super();
    }

    execute(): JQueryPromise<void> {
        const args = {
            nodeTag: this.nodeTag,
            maxUtilizedCores : this.maxUtilizedCores
        }

        const url = endpoints.global.rachisAdmin.adminLicenseSetLimit + this.urlEncodeArgs(args);

        return this.post(url, null, null, { dataType: undefined })
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to set new license limits", response.responseText, response.statusText);
            })
            .done(() => this.reportSuccess("Successfully set the new license limit"));
    }
}

export = setLicenseLimitsCommand; 

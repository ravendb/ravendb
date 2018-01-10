import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getEulaCommand extends commandBase {

    execute(): JQueryPromise<string> {
        const url = endpoints.global.license.licenseEula;
        return this.query<string>(url, null, null, null,  { dataType: undefined });
    }
}

export = getEulaCommand;

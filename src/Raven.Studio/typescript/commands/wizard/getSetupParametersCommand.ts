import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getSetupParametersCommand extends commandBase {

    execute(): JQueryPromise<Raven.Server.Commercial.SetupParameters> {      
        const url = endpoints.global.setup.setupParameters;
        
        return this.query<Raven.Server.Commercial.SetupParameters>(url, null)
            .fail((response: JQueryXHR) => this.reportError("Failed to get setup parameters", response.responseText, response.statusText));            
    }
}

export = getSetupParametersCommand;

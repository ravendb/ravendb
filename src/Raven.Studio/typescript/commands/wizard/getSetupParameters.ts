import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getSetupParameters extends commandBase {

    execute(): JQueryPromise<Raven.Server.Commercial.SetupParameters> {      
        const url = endpoints.global.setup.setupParameters;
        
        return this.query(url, null)
            .fail((response: JQueryXHR) => this.reportError("Failed to get setup parameters", response.responseText, response.statusText));            
    }
}

export = getSetupParameters;

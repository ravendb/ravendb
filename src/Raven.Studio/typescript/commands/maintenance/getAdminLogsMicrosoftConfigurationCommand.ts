import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import Dictionary = System.Collections.Generic.Dictionary;

class getAdminLogsMicrosoftConfigurationCommand extends commandBase {
    
    execute(): JQueryPromise<Dictionary<string, string>> {
        const url = endpoints.global.adminLogs.adminLogsMicrosoftConfiguration;
        
        return this.query<Dictionary<string, string>>(url, null)
            .fail((response: JQueryXHR) => this.reportError("Failed to get Microsoft logs configuration", response.responseText, response.statusText)) 
    }
}

export = getAdminLogsMicrosoftConfigurationCommand;

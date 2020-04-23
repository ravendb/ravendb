import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class saveDatabaseSettingsCommand extends commandBase {
   
    constructor(private db: database, private settingsToSave: Array<setttingsItem>) {
        super();
    }
    
    execute(): JQueryPromise<void> {

        const settingsObject = Object.assign({}, ...(this.settingsToSave.map(item => {
            if (item) {
                return { [item.key]: item.value };
            }
        })));
        
        const url = endpoints.global.adminConfiguration.adminConfigurationSettings;
        
        return this.put<void>(url, JSON.stringify(settingsObject), this.db)
            .done(() => this.reportSuccess("Database Settings were saved successfully"))
            .fail((response: JQueryXHR) => this.reportError("Failed to save Database Settings", response.responseText, response.statusText));
    }
}

export = saveDatabaseSettingsCommand;

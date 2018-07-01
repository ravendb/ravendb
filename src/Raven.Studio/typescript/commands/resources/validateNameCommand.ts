import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class validateNameCommand extends commandBase {

    constructor(private type: Raven.Server.Web.Studio.StudioTasksHandler.ItemType, private name: string, private dataPath?: string) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Utils.NameValidation> {
        const args = {
            type: this.type,
            name: this.name,
            dataPath: this.dataPath || ""
        };
        
        const url = endpoints.global.studioTasks.studioTasksIsValidName + this.urlEncodeArgs(args); 
        
        return this.query<Raven.Server.Utils.NameValidation>(url, null)
           .fail((response: JQueryXHR) => { 
               this.reportError(`Failed to validate the ${this.type.toLocaleLowerCase()} name`, response.responseText, response.statusText);
            });
    }
}

export = validateNameCommand;

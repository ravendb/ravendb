import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class deleteServerWideCustomSorterCommand extends commandBase {
    private readonly name: string;
    
    constructor(name: string) {
        super();
        this.name = name;
    }

    execute(): JQueryPromise<void> {
        const args = {
            name: this.name
        };
        
        const url = endpoints.global.adminSorters.adminSorters + this.urlEncodeArgs(args);
        
        return this.del<void>(url, null)
            .done(() => this.reportSuccess(`Deleted Server-Wide Custom Sorter: ${this.name}`))
            .fail((response: JQueryXHR) => this.reportError("Failed to delete Server-Wide Custom Sorter: " + this.name, response.responseText, response.statusText));
    }
}

export = deleteServerWideCustomSorterCommand;

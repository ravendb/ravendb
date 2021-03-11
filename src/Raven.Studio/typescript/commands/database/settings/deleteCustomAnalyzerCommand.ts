import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class deleteCustomAnalyzerCommand extends commandBase {

    constructor(private db: database, private name: string) {
        super();
    }

    execute(): JQueryPromise<void> {
        const args = {
            name: this.name
        };
        
        const url = endpoints.databases.adminAnalyzers.adminAnalyzers + this.urlEncodeArgs(args);
        
        return this.del<void>(url, null, this.db)
            .done(() => {
                this.reportSuccess(`Deleted custom analyzer: ${this.name}`);
            })
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to delete custom analyzer: " + this.name, response.responseText, response.statusText);
            });
    }
}

export = deleteCustomAnalyzerCommand; 


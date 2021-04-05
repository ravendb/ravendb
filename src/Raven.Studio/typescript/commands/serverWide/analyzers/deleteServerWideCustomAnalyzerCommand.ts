import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class deleteServerWideCustomAnalyzerCommand extends commandBase {

    constructor(private name: string) {
        super();
    }

    execute(): JQueryPromise<void> {
        const args = {
            name: this.name
        };
        
        const url = endpoints.global.adminAnalyzers.adminAnalyzers + this.urlEncodeArgs(args);
        
        return this.del<void>(url, null) 
            .done(() => this.reportSuccess(`Deleted Server-Wide Custom Analyzer: ${this.name}`))
            .fail((response: JQueryXHR) => this.reportError("Failed to delete Server-Wide Custom Analyzer: " + this.name, response.responseText, response.statusText));
    }
}

export = deleteServerWideCustomAnalyzerCommand; 


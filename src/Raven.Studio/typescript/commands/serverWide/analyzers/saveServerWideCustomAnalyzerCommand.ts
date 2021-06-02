import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class saveServerWideCustomAnalyzerCommand extends commandBase {

    constructor(private analyzerDto: Raven.Client.Documents.Indexes.Analysis.AnalyzerDefinition) {
        super();
    }

    execute(): JQueryPromise<void> {
        const url = endpoints.global.adminAnalyzers.adminAnalyzers;
        
        const payload = {
            Analyzers: [ this.analyzerDto ]
        };

        return this.put<void>(url, JSON.stringify(payload), null, { dataType: undefined })
            .done(() => this.reportSuccess(`Saved Server-Wide Custom Analyzer ${this.analyzerDto.Name}`))
            .fail((response: JQueryXHR) => this.reportError("Failed to save Server-Wide Custom Analyzer", response.responseText, response.statusText));
    }
}

export = saveServerWideCustomAnalyzerCommand; 


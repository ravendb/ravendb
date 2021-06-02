import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getServerWideCustomAnalyzersCommand extends commandBase {

    execute(): JQueryPromise<Array<Raven.Client.Documents.Indexes.Analysis.AnalyzerDefinition>> {
        const args = {
            namesOnly: false
        };

        const url = endpoints.global.analyzers.analyzers + this.urlEncodeArgs(args);

        return this.query<Array<Raven.Client.Documents.Indexes.Analysis.AnalyzerDefinition>>(url, null, null, x => x.Analyzers)
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to get Server-Wide Custom Analyzers", response.responseText, response.statusText);
            });
    }
}

export = getServerWideCustomAnalyzersCommand;

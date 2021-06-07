import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getCustomAnalyzersCommand extends commandBase {

    constructor(private db: database, private getNamesOnly: boolean = false) {
        super();
    }

    execute(): JQueryPromise<Array<Raven.Client.Documents.Indexes.Analysis.AnalyzerDefinition>> {
        let url = endpoints.databases.analyzers.analyzers;

        if (this.getNamesOnly) {
            const args = {
                namesOnly: this.getNamesOnly
            };
            
            url += this.urlEncodeArgs(args);
        }

        return this.query<Array<Raven.Client.Documents.Indexes.Analysis.AnalyzerDefinition>>(url, null, this.db, x => x.Analyzers)
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to get custom analyzers", response.responseText, response.statusText);
            });
    }
}

export = getCustomAnalyzersCommand;

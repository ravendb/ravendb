import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getCustomAnalyzersCommand extends commandBase {

    private readonly db: database;
    private readonly getNamesOnly: boolean;

    constructor(db: database, getNamesOnly = false) {
        super();
        this.db = db;
        this.getNamesOnly = getNamesOnly;
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

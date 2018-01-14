import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getIndexesProgressCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Documents.Indexes.IndexProgress[]> {
        const url = endpoints.databases.index.indexesProgress;
        const extractor = (response: resultsDto<Raven.Client.Documents.Indexes.IndexProgress>) => response.Results;
        return this.query(url, null, this.db, extractor)
            .fail((response: JQueryXHR) =>
                this.reportError("Failed to compute indexing progress!", response.responseText, response.statusText));
    }
} 

export = getIndexesProgressCommand;

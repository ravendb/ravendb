import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getTransformersCommand extends commandBase {
    constructor(private db: database, private skip = 0, private take = 1024) {
        super();
    }

    execute(): JQueryPromise<Array<Raven.Client.Documents.Transformers.TransformerDefinition>> {
        const args = {
            start: this.skip,
            pageSize: this.take
        };
        const url = endpoints.databases.transformer.transformers + this.urlEncodeArgs(args);
        const extractor = (response: resultsDto<Raven.Client.Documents.Transformers.TransformerDefinition>) => response.Results;
        return this.query(url, null, this.db, extractor);
    }
}

export = getTransformersCommand;

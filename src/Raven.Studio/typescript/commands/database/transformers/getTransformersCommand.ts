import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getTransformersCommand extends commandBase {
    constructor(private db: database, private skip = 0, private take = 1024) {
        super();
    }

    execute(): JQueryPromise<Array<Raven.Abstractions.Indexing.TransformerDefinition>> {
        const args = {
            start: this.skip,
            pageSize: this.take
        };
        const url = endpoints.databases.transformer.transformers + this.urlEncodeArgs(args);
        return this.query(url, null, this.db);
    }
}

export = getTransformersCommand;

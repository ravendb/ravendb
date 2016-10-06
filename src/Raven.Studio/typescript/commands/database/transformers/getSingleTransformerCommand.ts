import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getSingleTransformerCommand extends commandBase {

    constructor(private transformerName:string, private db:database) {
        super();
    }

    execute(): JQueryPromise<Raven.Abstractions.Indexing.TransformerDefinition> {
        const args = {
            name: this.transformerName
        };
        const url = endpoints.databases.transformer.transformers + this.urlEncodeArgs(args);
        return this.query<Raven.Abstractions.Indexing.TransformerDefinition>(url, null, this.db,
            (r: Raven.Abstractions.Indexing.TransformerDefinition[]) => r[0]);
    }
}

export = getSingleTransformerCommand;

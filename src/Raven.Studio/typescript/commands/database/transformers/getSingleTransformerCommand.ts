import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getSingleTransformerCommand extends commandBase {

    constructor(private transformerName:string, private db:database) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Documents.Transformers.TransformerDefinition> {
        const args = {
            name: this.transformerName
        };
        const url = endpoints.databases.transformer.transformers + this.urlEncodeArgs(args);
        return this.query<Raven.Client.Documents.Transformers.TransformerDefinition>(url, null, this.db,
            (r: resultsDto<Raven.Client.Documents.Transformers.TransformerDefinition>) => r.Results[0]);
    }
}

export = getSingleTransformerCommand;

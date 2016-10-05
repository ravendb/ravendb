import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import transformer = require("models/database/index/transformer");
import endpoints = require("endpoints");

class saveTransformerCommand extends commandBase {
    constructor(private transformer: transformer, private db: database) {
        super();
    }

    execute(): JQueryPromise<void> {
        const args = {
            name: this.transformer.name()
        };
        const url = endpoints.databases.transformer.transformers + this.urlEncodeArgs(args);
        const saveTransformerPutArgs = JSON.stringify(this.transformer.toDto());

        return this.put(url, saveTransformerPutArgs, this.db)
            .done(() => this.reportSuccess("Saved " + this.transformer.name()))
            .fail((result: JQueryXHR) => this.reportError("Unable to save transformer", result.responseText, result.statusText));

    }
    
}

export = saveTransformerCommand;

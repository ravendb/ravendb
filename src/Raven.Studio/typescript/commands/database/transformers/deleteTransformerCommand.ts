import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class deleteTransformerCommand extends commandBase {

    constructor(private transformerName: string, private db:database) {
        super();
    }

    execute(): JQueryPromise<void> {
        this.reportInfo("Deleting " + this.transformerName + "...");
        const args = {
            name : this.transformerName
        }

        const url = endpoints.databases.transformer.transformers + this.urlEncodeArgs(args);
        return this.del(url, null, this.db)
            .done(() => this.reportSuccess("Deleted " + this.transformerName))
            .fail((response: JQueryXHR) => this.reportError("Failed to delete transformer " + this.transformerName,
                response.responseText));
    }
}

 export = deleteTransformerCommand;

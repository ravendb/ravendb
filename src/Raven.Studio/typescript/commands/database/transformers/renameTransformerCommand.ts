import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class renameTransformerCommand extends commandBase {

    constructor(private existingName: string, private newName: string, private db: database) {
        super();
    }

    execute(): JQueryPromise<void> {
        return this.rename()
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to rename " + this.existingName, response.responseText, response.statusText);
            })
            .done(() => this.reportSuccess("Renamed " + this.existingName + " to " + this.newName));

    }

    private rename(): JQueryPromise<void> {
        const args = {
            name: this.existingName,
            newName: this.newName
        };
        const url = endpoints.databases.transformer.transformersRename + this.urlEncodeArgs(args);
        return this.post(url, null, this.db, { dataType: undefined });
    }
}

export = renameTransformerCommand;
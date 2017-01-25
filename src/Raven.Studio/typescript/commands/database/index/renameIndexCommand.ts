import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class renameIndexCommand extends commandBase {

    constructor(private existingIndexName: string, private newIndexName: string, private db: database) {
        super();
    }

    execute(): JQueryPromise<void> {
        return this.rename()
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to rename " + this.existingIndexName, response.responseText, response.statusText);
            })
            .done(() => {
                this.reportSuccess("Renamed " + this.existingIndexName + " to " + this.newIndexName);
            });
    }

    private rename(): JQueryPromise<void> {
        const args = {
            name: this.existingIndexName,
            newName: this.newIndexName
        };
        const url = endpoints.databases.index.indexesRename + this.urlEncodeArgs(args);
        return this.post(url, null, this.db, { dataType: undefined });
    }
}

export = renameIndexCommand;
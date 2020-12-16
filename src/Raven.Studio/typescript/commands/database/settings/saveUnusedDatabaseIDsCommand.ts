import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class saveUnusedDatabaseIDsCommand extends commandBase {

    constructor(private unusedDatabaseIDs: string[], private dbName: string) {
        super();
    }

    execute(): JQueryPromise<void> {
        const args = {
            name: this.dbName
        }

        const url = endpoints.global.adminDatabases.adminDatabasesUnusedIds + this.urlEncodeArgs(args);

        const payload = {
            DatabaseIds: this.unusedDatabaseIDs
        };

        return this.post(url, JSON.stringify(payload))
            .done(() => this.reportSuccess("Unused database IDs saved successfully"))
            .fail((response: JQueryXHR) => this.reportError("Failed to save the unused database IDs", response.responseText));
    }
}

export = saveUnusedDatabaseIDsCommand;

import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class validateExportDatabaseOptionsCommand extends commandBase {

    constructor(private smugglerOptions: Raven.Client.Smuggler.DatabaseSmugglerOptions, private db: database) {
        super();
    }

    execute(): JQueryPromise<void> {
        return this.post(endpoints.databases.smuggler.smugglerValidateOptions, JSON.stringify(this.smugglerOptions), this.db, { dataType: undefined });
    }
}

export = validateExportDatabaseOptionsCommand;

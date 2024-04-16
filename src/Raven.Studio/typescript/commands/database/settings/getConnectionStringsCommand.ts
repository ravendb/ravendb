import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
class getConnectionStringsCommand extends commandBase {

    private readonly db: database | string;
    
    constructor(db: database | string) {
        super();
        this.db = db;
    }

    execute(): JQueryPromise<GetConnectionStringsResult> {
        const url = endpoints.databases.ongoingTasks.adminConnectionStrings;

        return this.query<GetConnectionStringsResult>(url, null, this.db)
            .fail((response: JQueryXHR) => this.reportError("Failed to get connection strings", response.responseText, response.statusText));
    }
}

export = getConnectionStringsCommand;

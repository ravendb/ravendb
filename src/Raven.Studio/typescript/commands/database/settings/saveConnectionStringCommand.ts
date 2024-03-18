import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import { ConnectionStringDto } from "components/pages/database/settings/connectionStrings/connectionStringsTypes";

class saveConnectionStringCommand extends commandBase {
    private readonly db: database | string;
    private readonly connectionString: ConnectionStringDto;

    constructor(db: database | string, connectionString: ConnectionStringDto) {
        super();
        this.db = db;
        this.connectionString = connectionString;
    }

    execute(): JQueryPromise<void> {
        const url = endpoints.databases.ongoingTasks.adminConnectionStrings;

        return this.put<void>(url, JSON.stringify(this.connectionString), this.db)
            .fail((response: JQueryXHR) =>
                this.reportError("Failed to save connection string", response.responseText, response.statusText)
            )
            .done(() => this.reportSuccess(`Connection string was saved successfully`));
    }
}

export = saveConnectionStringCommand;

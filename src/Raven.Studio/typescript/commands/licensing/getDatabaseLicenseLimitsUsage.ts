import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import database = require("models/resources/database");

class getDatabaseLicenseLimitsUsage extends commandBase {

    private readonly db: database;

    constructor(db: database) {
        super();
        this.db = db;
    }

    execute(): JQueryPromise<Raven.Server.Commercial.DatabaseLicenseLimitsUsage> {
        const url = endpoints.databases.studioStats.studioLicenseLimitsUsage;
        
        return this.query<Raven.Server.Commercial.DatabaseLicenseLimitsUsage>(url, null, this.db)
            .fail((response: JQueryXHR) => this.reportError("Failed to get license limits usage", response.responseText));
    }
}

export = getDatabaseLicenseLimitsUsage;

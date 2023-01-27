import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import { DatabaseSharedInfo } from "components/models/databases";

class getDatabaseDetailedStatsCommand extends commandBase {

    private db: database | DatabaseSharedInfo;

    private location: databaseLocationSpecifier;

    private longWait = false;

    constructor(db: database | DatabaseSharedInfo, location: databaseLocationSpecifier, longWait = false) {
        super();
        this.longWait = longWait;
        this.location = location;
        this.db = db;
    }

    execute(): JQueryPromise<Raven.Client.Documents.Operations.DetailedDatabaseStatistics> {
        
        const url = endpoints.databases.stats.statsDetailed;
        const args = {
            ...this.location
        };
        
        return this.query<Raven.Client.Documents.Operations.DetailedDatabaseStatistics>(url, args, this.db, null, null, this.getTimeToAlert(this.longWait));
    }
}

export = getDatabaseDetailedStatsCommand;

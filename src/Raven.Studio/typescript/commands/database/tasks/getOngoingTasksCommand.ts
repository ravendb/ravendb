import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getOngoingTasksCommand extends commandBase {

    private readonly db: database;

    private readonly location: databaseLocationSpecifier;

    constructor(db: database, location: databaseLocationSpecifier) {
        super();
        this.location = location;
        this.db = db;
    }

    execute(): JQueryPromise<Raven.Server.Web.System.OngoingTasksResult> {
        const url = endpoints.databases.ongoingTasks.tasks;
        
        const args = {
            ...this.location
        };

        return this.query<Raven.Server.Web.System.OngoingTasksResult>(url, args, this.db);
    }
}

export = getOngoingTasksCommand;

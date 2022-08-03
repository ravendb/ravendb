import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getOngoingTasksCommand extends commandBase {

    constructor(private db: database, private location: databaseLocationSpecifier) {
        super();
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

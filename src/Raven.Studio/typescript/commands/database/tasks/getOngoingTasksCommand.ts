import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getOngoingTasksCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Web.System.OngoingTasksResult> {
        const url = endpoints.databases.ongoingTasks.tasks;

        return this.query<Raven.Server.Web.System.OngoingTasksResult>(url, null, this.db)
            .fail((response: JQueryXHR) => this.reportError("Failed to get ongoing tasks", response.responseText, response.statusText));
    }
}

export = getOngoingTasksCommand;
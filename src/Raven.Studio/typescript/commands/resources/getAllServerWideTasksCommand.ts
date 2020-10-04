import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getAllServerWideTasksCommand extends commandBase {

    constructor(private taskName?: string, private type?: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskType) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Web.System.AdminStudioServerWideHandler.ServerWideTasksResult> {
        const args = {
            taskName: this.taskName,
            type: this.type
        };

        const url = endpoints.global.adminStudioServerWide.adminServerWideTasks + this.urlEncodeArgs(args);

        return this.query<Raven.Server.Web.System.AdminStudioServerWideHandler.ServerWideTasksResult>(url, null)
            .fail((response: JQueryXHR) => {
                this.reportError(`Failed to get all Server-Wide tasks`,
                    response.responseText, response.statusText);
            });
    }
}

export = getAllServerWideTasksCommand;

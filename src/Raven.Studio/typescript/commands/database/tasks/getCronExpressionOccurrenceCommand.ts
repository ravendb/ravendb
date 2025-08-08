import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getCronExpressionOccurrenceCommand extends commandBase {
    constructor(private cronExpression: string,
                private databaseName?: string,
                private taskId?: number,
                private isFull?: boolean) {
        super();
    }
 
    execute(): JQueryPromise<Raven.Server.Web.Studio.StudioTasksHandler.NextCronExpressionOccurrence> {
        const args = {
            expression: this.cronExpression,
            database: this.databaseName,
            taskId: this.taskId,
            isFull: this.isFull
        };

        const url = endpoints.global.studioTasks.studioTasksNextCronExpressionOccurrence +
            this.urlEncodeArgs(args);

        return this.query<Raven.Server.Web.Studio.StudioTasksHandler.NextCronExpressionOccurrence>(url, null)
            .fail((response: JQueryXHR) => this.reportError(`Failed to get next occurrence of cron expression`, response.responseText, response.statusText));
    }
}

export = getCronExpressionOccurrenceCommand;

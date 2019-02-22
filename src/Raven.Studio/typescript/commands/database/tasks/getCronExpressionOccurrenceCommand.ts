import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class getCronExpressionOccurrenceCommand extends commandBase {
    constructor(private cronExpression: string) {
        super();
    }
 
    execute(): JQueryPromise<Raven.Server.Web.Studio.StudioTasksHandler.NextCronExpressionOccurrence> {
        const url = endpoints.global.studioTasks.studioTasksNextCronExpressionOccurrence +
            this.urlEncodeArgs({ expression: this.cronExpression });

        return this.query<Raven.Server.Web.Studio.StudioTasksHandler.NextCronExpressionOccurrence>(url, null)
            .fail((response: JQueryXHR) => this.reportError(`Failed to get next occurrence of cron expression`, response.responseText, response.statusText));
    }
}

export = getCronExpressionOccurrenceCommand;

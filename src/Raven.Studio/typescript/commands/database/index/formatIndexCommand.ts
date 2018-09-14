import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");


class formatIndexCommand extends commandBase {

    constructor(private db: database, private expression: string) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Web.Studio.StudioTasksHandler.FormattedExpression> {
        const url = endpoints.global.studioTasks.studioTasksFormat;
        const payload = {
            Expression: this.expression
        };
        return this.post(url, JSON.stringify(payload)).fail((response: JQueryXHR) => {
            this.reportError("Failed to format text!", response.responseText, response.statusText);
        });
    }
}

export = formatIndexCommand;

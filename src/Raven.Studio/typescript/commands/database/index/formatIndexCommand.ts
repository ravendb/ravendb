import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import FormatedExpression = Raven.Server.Web.Studio.StudioTasksHandler.FormatedExpression;

class formatIndexCommand extends commandBase {

    constructor(private db: database, private expressionAsString: string) {
        super();
    }

    execute(): JQueryPromise<FormatedExpression> {
        var url = endpoints.global.studioTasks.studioTasksFormat;
        var expression = { "Expression": this.expressionAsString };
        return this.post(url, JSON.stringify(expression));
    }
}

export = formatIndexCommand;

import commandBase = require("commands/commandBase");
import timeSeries = require("models/timeSeries/timeSeries");

class deleteTypesCommand extends commandBase {

    constructor(private types: string[], private ts: timeSeries) {
        super();
    }

    execute(): JQueryPromise<any> {
        var deletionTasks = new Array<JQueryPromise<any>>();;
        for (var i = 0; i < this.types.length; i++) {
            var deleteCommand = this.deleteType(this.types[i]);
            deletionTasks.push(deleteCommand);
        }

        var successMessage = this.types.length > 1 ? "Deleted " + this.types.length + " types" : "Deleted " + this.types[0];

        var combinedTask = $.when.apply($, deletionTasks)
            .done(() => this.reportSuccess(successMessage))
            .fail((response: JQueryXHR) => this.reportError("Failed to delete type", response.responseText, response.statusText));

        return combinedTask;
    }

    deleteType(type : string): JQueryPromise<any> {
        var url = "/types/" + encodeURIComponent(type);
        return this.del(url, null, this.ts, { dataType : undefined}, 9000 * this.types.length);
    }
}

export = deleteTypesCommand;  

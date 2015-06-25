import commandBase = require("commands/commandBase");
import counterStorage = require("models/counter/counterStorage");

class deleteCountersCommand extends commandBase {

    constructor(private counterIds: string[], private cs: counterStorage) {
        super();
    }

    execute(): JQueryPromise<any> {
        var deletionTasks = new Array<JQueryPromise<any>>();;
        for (var i = 0; i < this.counterIds.length; i++) {
            var deleteCommand = this.deleteCounter(this.counterIds[i]);
            deletionTasks.push(deleteCommand);
        }

        var successMessage = "Deleted " + (this.counterIds.length > 1 ?  + this.counterIds.length + " counters" : this.counterIds[0]);
        var failMessage = "Failed to delete " + (this.counterIds.length > 1 ? this.counterIds.length + " counters" : this.counterIds[0]);

        var combinedTask = $.when.apply($, deletionTasks)
            .done(x => this.reportSuccess(successMessage))
            .fail((response: JQueryXHR) => this.reportError(failMessage, response.responseText, response.statusText));

        return combinedTask;
    }

    deleteCounter(fileId : string): JQueryPromise<any> {
        var fileIdSplitted = fileId.split("/");
        var args = {
            groupName: fileIdSplitted[0],
            counterName: fileIdSplitted[1]
        };
        var url = "/delete/" + this.urlEncodeArgs(args);
        return this.del(url, null, this.cs, { dataType: undefined }, 9000 * this.counterIds.length);
    }

}

export = deleteCountersCommand;  
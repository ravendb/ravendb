import commandBase = require("commands/commandBase");
import counterStorage = require("models/counter/counterStorage");

class deleteCountersCommand extends commandBase {

    constructor(private groupAndNames: {groupName: string; counterName: string}[], private cs: counterStorage) {
        super();
    }

    execute(): JQueryPromise<any> {
        var deletionTasks = new Array<JQueryPromise<any>>();;
        for (var i = 0; i < this.groupAndNames.length; i++) {
            var deleteCommand = this.deleteCounter(this.groupAndNames[i]);
            deletionTasks.push(deleteCommand);
        }

        var successMessage = "Deleted " + (this.groupAndNames.length > 1 ?  + this.groupAndNames.length + " counters" : this.getCounterDeleteText(this.groupAndNames[0]));
        var failMessage = "Failed to delete " + (this.groupAndNames.length > 1 ? this.groupAndNames.length + " counters" : this.getCounterDeleteText(this.groupAndNames[0]));

        var combinedTask = $.when.apply($, deletionTasks)
            .done(() => this.reportSuccess(successMessage))
            .fail((response: JQueryXHR) => this.reportError(failMessage, response.responseText, response.statusText));

        return combinedTask;
    }

    deleteCounter(groupAndName: {groupName: string; counterName: string}): JQueryPromise<any> {
        var url = "/delete/" + this.urlEncodeArgs(groupAndName);
        return this.del(url, null, this.cs, { dataType: undefined }, 9000);
    }

	getCounterDeleteText(groupAndName: {groupName: string; counterName: string}): string {
		return "counter name: " + groupAndName.counterName + ", group: " + groupAndName.groupName;
	}
}

export = deleteCountersCommand;
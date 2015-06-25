import commandBase = require("commands/commandBase");
import counterStorage = require("models/counter/counterStorage");
import counterGroup = require("models/counter/counterGroup");

class deleteCountersByGroupCommand extends commandBase {

    constructor(private group: counterGroup, private cs: counterStorage) {
        super();
    }

    execute(): JQueryPromise<any> {
        var args = {
            groupName: this.group.isAllGroupsGroup ? "" : this.group.name
        };
        var url = "/delete-by-group/" + this.urlEncodeArgs(args);
        var deleteTask = this.del(url, null, this.cs, { dataType: undefined }, (9000 * this.group.countersCount()) / 2);
	    deleteTask.done((numOfDeletedCounters: number) => this.reportSuccess("Successfully deleted " + numOfDeletedCounters +  " counters in '" + this.group.name + "'"));
		deleteTask.fail((response: JQueryXHR) => this.reportError("Failed to delete '" + this.group.name + "'", response.responseText, response.statusText));
        return deleteTask;
    }
}

export = deleteCountersByGroupCommand;  
import commandBase = require("commands/commandBase");
import counterSummary = require("models/counter/counterSummary");
import counterStorage = require("models/counter/counterStorage");
import pagedResultSet = require("common/pagedResultSet");

class getCountersCommand extends commandBase {

    /**
    * @param counterStorage - the counter storage that is being used
    * @param skip - number of entries to skip
    * @param take - number of entries to take
    * @param counterGroupName - the counter group to take the entries from
    */
    constructor(private cs: counterStorage, private skip: number, private take: number, private group: string = null) {
        super();
    }

    execute(): JQueryPromise<pagedResultSet> {
        var args = {
            skip: this.skip,
            take: this.take,
            group: this.group
        };

        var url = "/counters";
        var doneTask = $.Deferred();
        var selector = (dtos: counterSummaryDto[]) => dtos.map(d => new counterSummary(d, this.group == null));
        var task = this.query(url, args, this.cs, selector);
        task.done((summaries: counterSummary[]) => doneTask.resolve(new pagedResultSet(summaries, summaries.length)));
        task.fail(xhr => doneTask.reject(xhr));
        return doneTask;
    }
}

export = getCountersCommand;  

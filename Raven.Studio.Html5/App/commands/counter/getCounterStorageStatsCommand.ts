import commandBase = require("commands/commandBase");
import counterStorage = require("models/counter/counterStorage");

class getCounterStorageStatsCommand extends commandBase {

    constructor(private cs: counterStorage, private longWait: boolean = false) {
        super();
    }

    execute(): JQueryPromise<counterStorageStatisticsDto> {
        var url = "/stats";
        return this.query<counterStorageStatisticsDto>(url, null, this.cs, null, this.getTimeToAlert(this.longWait));
    }
}

export = getCounterStorageStatsCommand;

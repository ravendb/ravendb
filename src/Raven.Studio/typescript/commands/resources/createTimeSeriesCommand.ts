 import commandBase = require("commands/commandBase");

class createTimeSeriesCommand extends commandBase {

    /**
    * @param timeSeriesName The file system name we are creating.
    */
    constructor(private timeSeriesName: string, private settings: {}) {
        super();

        if (!timeSeriesName) {
            this.reportError("Time series must have a name!");
            throw new Error("Time series must have a name!");
        }
    }

    execute(): JQueryPromise<any> {

        this.reportInfo("Creating Time Series '" + this.timeSeriesName + "'");

        var timeSeriesDoc = {
            "Settings": this.settings,
            "Disabled": false
        };

        var url = "/admin/ts/" + this.timeSeriesName;//TODO: use endpoints
        
        var createTask = this.put(url, JSON.stringify(timeSeriesDoc), null, { dataType: undefined });
        createTask.done(() => this.reportSuccess(this.timeSeriesName + " created"));
        createTask.fail((response: JQueryXHR) => this.reportError("Failed to create time series", response.responseText, response.statusText));

        return createTask;
    }
}

export = createTimeSeriesCommand;

import commandBase = require("commands/commandBase");
import timeSeries = require("models/timeSeries/timeSeries");

class deletePointsCommand extends commandBase {

    constructor(private points: pointIdDto[], private ts: timeSeries) {
        super();
    }

    execute(): JQueryPromise<any> {
        var successMessage = this.points.length > 1 ? this.points.length + " points deleted" : "Point deleted";
    
        var task = this.del("/delete-points", JSON.stringify(this.points), this.ts, null, 9000 * this.points.length)
            .done(x => this.reportSuccess(successMessage))
            .fail((response: JQueryXHR) => this.reportError("Failed to delete points", response.responseText, response.statusText));

        return task;
    }
}

export = deletePointsCommand;  
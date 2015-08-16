import commandBase = require("commands/commandBase");
import timeSeries = require("models/timeSeries/timeSeries");

class putTypeCommand extends commandBase {

    constructor(private type: string, private fields: string[], private ts: timeSeries) {
        super();
    }

    execute(): JQueryPromise<bulkDocumentDto[]> {
        var url = "/types/" + this.type;
        var dto: timeSeriesTypeDto = { Type: this.type, Fields: this.fields, KeysCount: 0};
        return this.put(url, JSON.stringify(dto), this.ts, { dataType: undefined })
            .done(() => this.reportSuccess("Type saved"))
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to save type", response.responseText, response.statusText);
            });
    }
}

export = putTypeCommand;
import commandBase = require("commands/commandBase");
import timeSeries = require("models/timeSeries/timeSeries");

class putPointCommand extends commandBase {

    constructor(private type: string, private key: string, private at: string, private values: number[], private ts: timeSeries) {
        super();
    }

    execute(): JQueryPromise<bulkDocumentDto[]> {
        var url = "/append/" + this.type;
        return this.put(url, JSON.stringify({ type: this.type, key: this.key, at: this.at, values: this.values }), this.ts)
            .done(() => this.reportSuccess("Point saved"))
            .fail((response: JQueryXHR) => {
                debugger
                this.reportError("Failed to save point", response.responseText, response.statusText);
            });
    }
}

export = putPointCommand;
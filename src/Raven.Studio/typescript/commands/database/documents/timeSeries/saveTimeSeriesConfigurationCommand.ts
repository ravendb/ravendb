import database = require("models/resources/database");
import commandBase = require("commands/commandBase");
import endpoint = require("endpoints");

class saveTimeSeriesConfigurationCommand extends commandBase {
    constructor(private db: database, private configuration: Raven.Client.Documents.Operations.TimeSeries.TimeSeriesConfiguration) {
        super();
    }

    execute(): JQueryPromise<updateDatabaseConfigurationsResult> {
        const url = endpoint.databases.timeSeries.adminTimeseriesConfig;
        const args = ko.toJSON(this.configuration);
        return this.post<updateDatabaseConfigurationsResult>(url, args, this.db)
            .fail((response: JQueryXHR) => this.reportError("Failed to save time series configuration", response.responseText, response.statusText));

    }
}

export = saveTimeSeriesConfigurationCommand;

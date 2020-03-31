import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getTimeSeriesConfigurationCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Documents.Operations.TimeSeries.TimeSeriesConfiguration> {

        const deferred = $.Deferred<Raven.Client.Documents.Operations.TimeSeries.TimeSeriesConfiguration>();
        const url = endpoints.databases.timeSeries.timeseriesConfig;
        this.query(url, null, this.db)
            .done((config: Raven.Client.Documents.Operations.TimeSeries.TimeSeriesConfiguration) => deferred.resolve(config))
            .fail((xhr: JQueryXHR) => {
                if (xhr.status === 404) {
                    deferred.resolve(null);
                } else {
                    deferred.reject(xhr);
                    this.reportError("Failed to get time series configuration", xhr.responseText, xhr.statusText);
                }
                
            });

        return deferred;
    }
}

export = getTimeSeriesConfigurationCommand;

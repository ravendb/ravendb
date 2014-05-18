import getStatusDebugMetricsCommand = require("commands/getStatusDebugMetricsCommand");
import appUrl = require("common/appUrl");
import database = require("models/database");
import viewModelBase = require("viewmodels/viewModelBase");


class statusDebugMetrics extends viewModelBase {
    data = ko.observable<statusDebugMetricsDto>();
    percentiles = ko.computed<Array<any>>(() => {
        var result = [];
        var d = this.data();
        if (d) {
            for (var prop in d.RequestsDuration.Percentiles) {
                var v = d.RequestsDuration.Percentiles[prop];
                result.push({ "key": prop, "value": v });
            }
        }
        return result;
    });

    activate(args) {
        super.activate(args);

        this.activeDatabase.subscribe(() => this.fetchStatusDebugMetrics());
        return this.fetchStatusDebugMetrics();
    }

    fetchStatusDebugMetrics(): JQueryPromise<statusDebugMetricsDto> {
        var db = this.activeDatabase();
        if (db) {
            return new getStatusDebugMetricsCommand(db)
                .execute()
                .done((results: statusDebugMetricsDto) => this.data(results));
        }

        return null;
    }
}

export = statusDebugMetrics;
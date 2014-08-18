import getStatusDebugSqlReplicationCommand = require("commands/getStatusDebugSqlReplicationCommand");
import appUrl = require("common/appUrl");
import database = require("models/database");
import viewModelBase = require("viewmodels/viewModelBase");


class statusDebugSqlReplication extends viewModelBase {
    data = ko.observable<sqlReplicationStatsDto[]>();
    rateMetrics: KnockoutComputed<any[]>;
    metricsTables: KnockoutComputed<string[]>;

    constructor() {
        super();
       /* this.metricsTables = ko.computed<string[]>(() => {
            var tables = ['All Tables'];
            var metricsData = this.data().Metrics;
            if (!!metricsData) {
                
            }

            return tables;
        });
        this.rateMetrics = ko.computed<any[]>(() => {
            var a = [];

            return a;
        });*/
    }
    activate(args) {
        super.activate(args);

        this.activeDatabase.subscribe(() => this.fetchSqlReplicationStats());
        return this.fetchSqlReplicationStats();
    }

    fetchSqlReplicationStats(): JQueryPromise<sqlReplicationStatisticsDto[]> {
        var db = this.activeDatabase();
        if (db) {
            return new getStatusDebugSqlReplicationCommand(db)
                .execute()
                .done((results: sqlReplicationStatisticsDto[]) => {
                this.data(results);
            });
        }

        return null;
    }
}

export = statusDebugSqlReplication;
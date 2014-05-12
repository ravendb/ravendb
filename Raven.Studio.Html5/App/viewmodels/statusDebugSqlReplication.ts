import getStatusDebugSqlReplicationCommand = require("commands/getStatusDebugSqlReplicationCommand");
import appUrl = require("common/appUrl");
import database = require("models/database");
import viewModelBase = require("viewmodels/viewModelBase");


class statusDebugSqlReplication extends viewModelBase {
    data = ko.observable<sqlReplicationStatisticsDto[]>();

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
                .done((results: sqlReplicationStatisticsDto[]) => this.data(results));
        }

        return null;
    }
}

export = statusDebugSqlReplication;
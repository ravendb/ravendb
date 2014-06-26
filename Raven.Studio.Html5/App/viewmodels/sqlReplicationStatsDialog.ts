import getStatusDebugSqlReplicationCommand = require("commands/getStatusDebugSqlReplicationCommand");
import appUrl = require("common/appUrl");
import database = require("models/database");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import dialog = require("plugins/dialog");

class sqlReplicationStatsDialog extends dialogViewModelBase {
    data = ko.observable<sqlReplicationStatisticsDto[]>();
    repData= ko.observable<sqlReplicationStatisticsDto>();

    constructor(private activeDatabase: database, private sqlReplicationName:string) {
        super();
        this.fetchSqlReplicationStats();
    }

    activate(args) {
        return this.fetchSqlReplicationStats();
    }

    cancel() {
        dialog.close(this);
    }

    fetchSqlReplicationStats(): JQueryPromise<sqlReplicationStatisticsDto[]> {
        var db = this.activeDatabase;
        if (db) {
            return new getStatusDebugSqlReplicationCommand(db)
                .execute()
                .done((results: sqlReplicationStatisticsDto[]) => {
                    this.data(results);
                    if (!!results && results.length > 0) {
                        this.repData(results[0]);
                    } 
            });
        }

        return null;
    }
}

export = sqlReplicationStatsDialog;
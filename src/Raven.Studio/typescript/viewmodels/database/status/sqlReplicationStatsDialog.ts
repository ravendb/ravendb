import database = require("models/resources/database");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import dialog = require("plugins/dialog");
import getSqlReplicationStatsCommand = require("commands/database/studio/getSqlReplicationStatsCommand");

class sqlReplicationStatsDialog extends dialogViewModelBase {
    repData = ko.observable<Raven.Server.Documents.SqlReplication.SqlReplicationStatistics>();

    constructor(private activeDatabase: database, private sqlReplicationName:string) {
        super();
    }

    activate(args: any) {
        this.fetchSqlReplicationStats();
    }

    cancel() {
        dialog.close(this);
    }

    fetchSqlReplicationStats(): JQueryPromise<Raven.Server.Documents.SqlReplication.SqlReplicationStatistics> {
        return new getSqlReplicationStatsCommand(this.activeDatabase, this.sqlReplicationName)
            .execute()
            .done((result: Raven.Server.Documents.SqlReplication.SqlReplicationStatistics) => {
                this.repData(result);
            }).fail(() => this.repData(null));
    }
}

export = sqlReplicationStatsDialog;

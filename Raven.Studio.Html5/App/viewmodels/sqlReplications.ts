import database = require("models/database");
import sqlReplication = require("models/sqlReplication");
import viewModelBase = require("viewmodels/viewModelBase");
import getSqlReplicationsCommand = require("commands/getSqlReplicationsCommand");
import appUrl = require("common/appUrl");
import sqlReplicationStatsDialog = require("viewmodels/sqlReplicationStatsDialog");
import app = require("durandal/app");
import document = require("models/document");
import deleteDocuments = require("viewmodels/deleteDocuments");
import messagePublisher = require("common/messagePublisher");
import resetSqlReplicationCommand = require("commands/resetSqlReplicationCommand");

class sqlReplications extends viewModelBase {

    replications = ko.observableArray<sqlReplication>();
    loadedSqlReplications: sqlReplication[];
    static sqlReplicationsSelector = "#sqlReplications";

    constructor() {
        super();
    }

    showStats(replicationName:string) {
        var viewModel = new sqlReplicationStatsDialog(this.activeDatabase(), replicationName);
        app.showDialog(viewModel);
    }

    getSqlReplicationUrl(sqlReplicationName: string) {
        return appUrl.forEditSqlReplication(sqlReplicationName, this.activeDatabase());
    }

    getSqlReplicationConnectionStringsUrl(sqlReplicationName: string) {
        return appUrl.forSqlReplicationConnections(this.activeDatabase());
    }

    canActivate(args: any): JQueryPromise<any> {
        var deferred = $.Deferred();

        var db = this.activeDatabase();
        if (db) {
           this.fetchSqlReplications(db)
                .done(() => deferred.resolve({ can: true }))
                .fail(() => deferred.resolve({ redirect: appUrl.forSettings(this.activeDatabase()) }));
        }
        return deferred;
    }

    activate(args) {
        super.activate(args);
        this.updateHelpLink('J9VD59');
    }

    compositionComplete() {
        super.compositionComplete();
    }

    removeSqlReplication(sr: sqlReplication) {
        var newDoc = new document(sr);

        if (newDoc) {
            var viewModel = new deleteDocuments([newDoc]);
            viewModel.deletionTask.done(() => {
                this.fetchSqlReplications(this.activeDatabase());
            });
            app.showDialog(viewModel, sqlReplications.sqlReplicationsSelector);

        }
    }

    resetSqlReplication(replicationId: string) {
        app.showMessage("You are about to reset this SQL Replication, forcing replication of all collection items", "SQL Replication Reset", ["Cancel", "Reset"])
            .then((dialogResult: string) => {
                if (dialogResult === "Reset") {
                    new resetSqlReplicationCommand(this.activeDatabase(), replicationId).execute()
                        .done(() => messagePublisher.reportSuccess("SQL replication " + replicationId + " was reset successfully!"))
                        .fail(() => messagePublisher.reportError("SQL replication " + replicationId + " failed to reset!"));
                }
            });
        
    }

    itemNumber = (index) => {
        return index + 1;
    }

    private fetchSqlReplications(db: database): JQueryPromise<any> {
        return new getSqlReplicationsCommand(db)
            .execute()
            .done((results: sqlReplication[])=> {
                this.replications(results);
            });
    }
   
}

export = sqlReplications; 
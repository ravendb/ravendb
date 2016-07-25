import database = require("models/resources/database");
import sqlReplication = require("models/database/sqlReplication/sqlReplication");
import viewModelBase = require("viewmodels/viewModelBase");
import getSqlReplicationsCommand = require("commands/database/sqlReplication/getSqlReplicationsCommand");
import appUrl = require("common/appUrl");
import sqlReplicationStatsDialog = require("viewmodels/database/status/sqlReplicationStatsDialog");
import app = require("durandal/app");
import document = require("models/database/documents/document");
import deleteDocuments = require("viewmodels/common/deleteDocuments");
import messagePublisher = require("common/messagePublisher");
import resetSqlReplicationCommand = require("commands/database/sqlReplication/resetSqlReplicationCommand");
import toggleDisable = require("commands/database/sqlReplication/toggleDisable");

class sqlReplications extends viewModelBase {

    replications = ko.observableArray<sqlReplication>();
    loadedSqlReplications: sqlReplication[];
    static sqlReplicationsSelector = "#sqlReplications";
    areAllSqlReplicationsDisabled: KnockoutComputed<boolean>;
    areAllSqlReplicationsEnabled: KnockoutComputed<boolean>;
    searchText = ko.observable<string>("");
    summary: KnockoutComputed<string>;

    constructor() {
        super();

        this.areAllSqlReplicationsDisabled = ko.computed(() => {
            var replications = this.replications();
            for (var i = 0; i < replications.length; i++) {
                if (replications[i].disabled() === false)
                    return false;
            }

            return true;
        });

        this.areAllSqlReplicationsEnabled = ko.computed(() => {
            var replications = this.replications();
            for (var i = 0; i < replications.length; i++) {
                if (replications[i].disabled())
                    return false;
            }

            return true;
        });

        this.searchText.extend({ throttle: 200 }).subscribe(() => this.filterSqlReplications());

        this.summary = ko.computed(() => {
            var summary = "";
            if (this.replications().length === 0) {
                return summary;
            }

            var visibleSqlReplications = this.replications().filter(x => x.isVisible());
            summary += visibleSqlReplications.length + " SQL Replication";
            if (visibleSqlReplications.length > 1) {
                summary += "s";
            }

            var disabled = visibleSqlReplications.filter(x => x.disabled()).length;
            if (disabled > 0) {
                summary += " (" + disabled + " disabled)";
            }
            return summary;
        });
    }

    private filterSqlReplications() {
        var filter = this.searchText();
        var filterLower = filter.toLowerCase();
        this.replications().forEach((sql: sqlReplication) => {
            var isMatch = (!filter || (sql.name().toLowerCase().indexOf(filterLower) >= 0));
            sql.isVisible(isMatch);
        });
    }

    private toggleDisable(disable: boolean) {
        var self = this;
        var action = disable ? "disable" : "enable";
        var actionCapitalized = action.capitalizeFirstLetter();
        app.showMessage("Are you sure that you want to " + action + " all SQL Replications?", 
                actionCapitalized + " SQL Replications", ["Cancel", actionCapitalized])
            .then((dialogResult: string) => {
                if (dialogResult === actionCapitalized) {
                    new toggleDisable(self.activeDatabase(), disable).execute()
                        .done(() => {
                            this.replications().forEach(x => x.disabled(disable));
                            messagePublisher.reportSuccess("Successfully " + action + "d all SQL replications");
                        });
                }
            });
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

    activate(args: any) {
        super.activate(args);
        this.updateHelpLink('J9VD59');
    }

    removeSqlReplication(sr: sqlReplication) {
        var newDoc = new document(sr);

        if (newDoc) {
            var viewModel = new deleteDocuments([newDoc]);
            viewModel.deletionTask.done(() => {
                this.replications.remove(sr);
                //this.fetchSqlReplications(this.activeDatabase());
            });
            app.showDialog(viewModel, sqlReplications.sqlReplicationsSelector);

        }
    }

    resetSqlReplication(replicationId: string) {
        app.showMessage("You are about to reset this SQL Replication, forcing replication of all collection items", "SQL Replication Reset", ["Cancel", "Reset"])
            .then((dialogResult: string) => {
                if (dialogResult === "Reset") {
                    new resetSqlReplicationCommand(this.activeDatabase(), replicationId).execute()
                        .done(() => messagePublisher.reportSuccess("SQL replication " + replicationId + " was reset successfully!"));
                }
            });
        
    }

    itemNumber = (index: number) => {
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

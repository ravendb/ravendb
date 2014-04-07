import viewModelBase = require("viewmodels/viewModelBase");
import replicationsSetup = require("models/replicationsSetup");
import replicationConfig = require("models/replicationConfig")
import replicationDestination = require("models/replicationDestination");
import getDatabaseStatsCommand = require("commands/getDatabaseStatsCommand");
import getReplicationsCommand = require("commands/getReplicationsCommand");
import saveReplicationDocumentCommand = require("commands/saveReplicationDocumentCommand");
import getAutomaticConflictResolutionDocumentCommand = require("commands/getAutomaticConflictResolutionDocumentCommand");
import saveAutomaticConflictResolutionDocument = require("commands/saveAutomaticConflictResolutionDocument");
import appUrl = require("common/appUrl");

class replications extends viewModelBase {

    replicationConfig = ko.observable<replicationConfig>(new replicationConfig({ DocumentConflictResolution: "None", AttachmentConflictResolution: "None" }));
    replicationsSetup = ko.observable<replicationsSetup>(new replicationsSetup({ Destinations: [], Source: null }));
    
    replicationConfigDirtyFlag = new ko.DirtyFlag([]);
    replicationsSetupDirtyFlag = new ko.DirtyFlag([]);

    isConfigSaveEnabled: KnockoutComputed<boolean>;
    isSetupSaveEnabled: KnockoutComputed<boolean>;

    canActivate(args: any): JQueryPromise<any> {
        var deferred = $.Deferred();
        var db = this.activeDatabase();
        if (db) {
            var automaticConflictResolution = new getAutomaticConflictResolutionDocumentCommand(db).execute();
            var replicationDestinations = new getReplicationsCommand(db).execute();

            $.when(automaticConflictResolution, replicationDestinations)
                .done((repConfig, repSetup)=> {
                    if (repConfig != null) {
                        this.replicationConfig(new replicationConfig(repConfig));
                    }
                    if (repSetup != null) {
                        this.replicationsSetup(new replicationsSetup(repSetup));
                    }
                    deferred.resolve({ can: true });
                })
                .fail(() => deferred.resolve({ redirect: appUrl.forIndexes(this.activeDatabase()) }));
        }
        return deferred;
    }

    activate() {
        var self = this;
        this.replicationConfigDirtyFlag = new ko.DirtyFlag([this.replicationConfig]);
        this.isConfigSaveEnabled = ko.computed(function () {
            return self.replicationConfigDirtyFlag().isDirty();
        });
        this.replicationsSetupDirtyFlag = new ko.DirtyFlag([this.replicationsSetup, this.replicationsSetup().destinations()]);
        this.isSetupSaveEnabled = ko.computed(function () {
            return self.replicationsSetupDirtyFlag().isDirty();
        });

        var combinedFlag = ko.computed(function () {
            return (self.replicationConfigDirtyFlag().isDirty() || self.replicationsSetupDirtyFlag().isDirty());
        });
        viewModelBase.dirtyFlag = new ko.DirtyFlag([combinedFlag]);
    }

    createNewDestination() {
        this.replicationsSetup().destinations.unshift(replicationDestination.empty());
    }

    removeDestination(repl: replicationDestination) {
        this.replicationsSetup().destinations.remove(repl);
    }

    toggleUserCredentials(destination: replicationDestination) {
        destination.isUserCredentials.toggle();
    }

    toggleApiKeyCredentials(destination: replicationDestination) {
        destination.isApiKeyCredentials.toggle();
    }

    saveChanges() {
        if (this.replicationsSetup().source()) {
            this.saveReplicationSetup();
        } else {
            var db = this.activeDatabase();
            if (db) {
                new getDatabaseStatsCommand(db)
                    .execute()
                    .done(result=> {
                        this.prepareAndSaveReplicationSetup(result.DatabaseId);
                });
            }
        }
    }

    private prepareAndSaveReplicationSetup(source: string) {
        this.replicationsSetup().source(source);
        this.saveReplicationSetup();
    }

    private saveReplicationSetup() {
        var db = this.activeDatabase();
        if (db) {
            new saveReplicationDocumentCommand(this.replicationsSetup().toDto(), db)
                .execute()
                .done(() => this.replicationsSetupDirtyFlag().reset() );
        }
    }

    saveAutomaticConflictResolutionSettings() {
        var db = this.activeDatabase();
        if (db) {
            new saveAutomaticConflictResolutionDocument(this.replicationConfig().toDto(), db)
                .execute()
                .done(() => this.replicationConfigDirtyFlag().reset() );
        }
    }
}

export = replications; 
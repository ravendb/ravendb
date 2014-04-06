import viewModelBase = require("viewmodels/viewModelBase");
import replicationsSetup = require("models/replicationsSetup");
import replicationConfig = require("models/replicationConfig")
import replicationDestination = require("models/replicationDestination");
import getDatabaseStatsCommand = require("commands/getDatabaseStatsCommand");
import getReplicationsCommand = require("commands/getReplicationsCommand");
import saveReplicationDocumentCommand = require("commands/saveReplicationDocumentCommand");
import getAutomaticConflictResolutionDocumentCommand = require("commands/getAutomaticConflictResolutionDocumentCommand");
import saveAutomaticConflictResolutionDocument = require("commands/saveAutomaticConflictResolutionDocument");

class replications extends viewModelBase {

    replicationConfig = ko.observable<replicationConfig>();
    replicationsSetup = ko.observable<replicationsSetup>();
    
    replicationConfigDirtyFlag = ko.observable(new ko.DirtyFlag([]));
    replicationsSetupDirtyFlag = ko.observable(new ko.DirtyFlag([]));

    activate() {

        this.replicationConfig(new replicationConfig({DocumentConflictResolution: "None", AttachmentConflictResolution: "None"}));
        this.replicationsSetup(new replicationsSetup({ Destinations: [], Source: null }));

        var db = this.activeDatabase();
        if (db) {
            var automaticConflictResolution = new getAutomaticConflictResolutionDocumentCommand(db).execute();
            var replicationDestinations = new getReplicationsCommand(db).execute();

            var self = this;
            $.when(automaticConflictResolution, replicationDestinations)
                .done((repConfig, repSetup) => {
                    this.replicationConfig(new replicationConfig(repConfig));
                    if (repSetup != null) {
                        this.replicationsSetup(new replicationsSetup(repSetup));
                    }
                    this.replicationConfigDirtyFlag = ko.observable(new ko.DirtyFlag([this.replicationConfig]));
                    this.replicationsSetupDirtyFlag = ko.observable(new ko.DirtyFlag([this.replicationsSetup, this.replicationsSetup().destinations()]));
                    var combinedFlag = ko.computed(function () {
                        return (self.replicationConfigDirtyFlag()().isDirty() || self.replicationsSetupDirtyFlag()().isDirty());
                    });
                    viewModelBase.dirtyFlag = new ko.DirtyFlag([combinedFlag]);
                });  
        }
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
                .done(() => this.replicationsSetupDirtyFlag()().reset() );
        }
    }

    saveAutomaticConflictResolutionSettings() {
        var db = this.activeDatabase();
        if (db) {
            new saveAutomaticConflictResolutionDocument(this.replicationConfig().toDto(), db)
                .execute()
                .done(() => this.replicationConfigDirtyFlag()().reset() );
        }
    }
}

export = replications; 
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
    

    activate() {

        this.replicationConfig(new replicationConfig({DocumentConflictResolution: "None", AttachmentConflictResolution: "None"}));

        var db = this.activeDatabase();
        if (db) {
            new getAutomaticConflictResolutionDocumentCommand(db)
                .execute()
                .done(result => this.replicationConfig(new replicationConfig(result)));
        }

        this.replicationsSetup(new replicationsSetup({ Destinations: [], Source: null }));

        var db = this.activeDatabase();
        if (db) {
            new getReplicationsCommand(db)
                .execute()
                .done(result => this.replicationsSetup(new replicationsSetup(result)));
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
                    .done(result => this.prepareAndSaveReplicationSetup(result.DatabaseId));
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
            new saveReplicationDocumentCommand(this.replicationsSetup().toDto(), db).execute();
        }
    }

    saveAutomaticConflictResolutionSettings() {
        var db = this.activeDatabase();
        if (db) {
            new saveAutomaticConflictResolutionDocument(this.replicationConfig().toDto(), db).execute();
        }
    }
}

export = replications; 
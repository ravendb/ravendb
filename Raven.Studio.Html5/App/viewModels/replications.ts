import viewModelBase = require("viewmodels/viewModelBase");
import replicationsSetup = require("models/replicationsSetup");
import replicationDestination = require("models/replicationDestination");
import getDatabaseStatsCommand = require("commands/getDatabaseStatsCommand");
import getReplicationsCommand = require("commands/getReplicationsCommand");
import saveReplicationDocument = require("commands/saveReplicationDocument");

class replications extends viewModelBase {

    replicationsSetup = ko.observable<replicationsSetup>();

    activate() {
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
            new saveReplicationDocument(this.replicationsSetup().toDto(), db).execute();
        }
    }
}

export = replications; 
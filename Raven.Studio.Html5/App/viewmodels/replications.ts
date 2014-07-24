import viewModelBase = require("viewmodels/viewModelBase");
import replicationsSetup = require("models/replicationsSetup");
import replicationConfig = require("models/replicationConfig")
import replicationDestination = require("models/replicationDestination");
import getDatabaseStatsCommand = require("commands/getDatabaseStatsCommand");
import getReplicationsCommand = require("commands/getReplicationsCommand");
import updateServerPrefixHiLoCommand = require("commands/updateServerPrefixHiLoCommand");
import saveReplicationDocumentCommand = require("commands/saveReplicationDocumentCommand");
import getAutomaticConflictResolutionDocumentCommand = require("commands/getAutomaticConflictResolutionDocumentCommand");
import saveAutomaticConflictResolutionDocument = require("commands/saveAutomaticConflictResolutionDocument");
import getServerPrefixForHiLoCommand = require("commands/getServerPrefixForHiLoCommand");
import appUrl = require("common/appUrl");

class replications extends viewModelBase {

    replicationConfig = ko.observable<replicationConfig>(new replicationConfig({ DocumentConflictResolution: "None", AttachmentConflictResolution: "None" }));
    replicationsSetup = ko.observable<replicationsSetup>(new replicationsSetup({ Destinations: [], Source: null }));

    prefixForHilo = ko.observable<string>();
    
    replicationConfigDirtyFlag = new ko.DirtyFlag([]);
    replicationsSetupDirtyFlag = new ko.DirtyFlag([]);
    prefixHiloDirtyFlag = new ko.DirtyFlag([]);

    isConfigSaveEnabled: KnockoutComputed<boolean>;
    isSetupSaveEnabled: KnockoutComputed<boolean>;

    canActivate(args: any): JQueryPromise<any> {
        var deferred = $.Deferred();
        var db = this.activeDatabase();
        if (db) {
            $.when(this.fetchAutomaticConflictResolution(db), this.fetchReplications(db))
                .done(() => deferred.resolve({ can: true }) )
                .fail(() => deferred.resolve({ redirect: appUrl.forSettings(db) }));
        }
        return deferred;
    }

    activate(args) {
        super.activate(args);
        
        new getServerPrefixForHiLoCommand(this.activeDatabase())
            .execute()
            .done((result) => this.prefixForHilo(result));

        var self = this;
        this.replicationConfigDirtyFlag = new ko.DirtyFlag([this.replicationConfig]);
        this.isConfigSaveEnabled = ko.computed(() => {
            return self.replicationConfigDirtyFlag().isDirty();
        });
        this.replicationsSetupDirtyFlag = new ko.DirtyFlag([this.replicationsSetup, this.replicationsSetup().destinations(), this.replicationConfig]);
        this.isSetupSaveEnabled = ko.computed(() => {
            return self.replicationsSetupDirtyFlag().isDirty() ;
        });
        this.prefixHiloDirtyFlag = new ko.DirtyFlag([this.prefixForHilo]);

        var combinedFlag = ko.computed(() => {
            return (self.replicationConfigDirtyFlag().isDirty() || self.replicationsSetupDirtyFlag().isDirty() || self.prefixHiloDirtyFlag().isDirty());
        });
        this.dirtyFlag = new ko.DirtyFlag([combinedFlag]);
    }

    fetchAutomaticConflictResolution(db): JQueryPromise<any> {
        var deferred = $.Deferred();
        new getAutomaticConflictResolutionDocumentCommand(db)
            .execute()
            .done(repConfig => this.replicationConfig(new replicationConfig(repConfig)))
            .always(() => deferred.resolve({ can: true }));
        return deferred;
    }

    fetchReplications(db): JQueryPromise<any> {
        var deferred = $.Deferred();
        new getReplicationsCommand(db)
            .execute()
            .done(repSetup => this.replicationsSetup(new replicationsSetup(repSetup)))
            .always(() => deferred.resolve({ can: true }));
        return deferred;
    }

    createNewDestination() {
        var db = this.activeDatabase();
        this.replicationsSetup().destinations.unshift(replicationDestination.empty(db.name));
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
        if (this.isConfigSaveEnabled())
            this.saveAutomaticConflictResolutionSettings();
        if (this.isSetupSaveEnabled()) {
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

    saveServerPrefixForHiLo() {
        new updateServerPrefixHiLoCommand(this.prefixForHilo(), this.activeDatabase())
            .execute()
            .done(() => this.prefixHiloDirtyFlag().reset());
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
import viewModelBase = require("viewmodels/viewModelBase");
import replicationsSetup = require("models/replicationsSetup");
import replicationConfig = require("models/replicationConfig")
import replicationDestination = require("models/replicationDestination");
import getDatabaseStatsCommand = require("commands/getDatabaseStatsCommand");
import getGlobalConfigReplicationsCommand = require("commands/getGlobalConfigReplicationsCommand");
import saveReplicationDocumentCommand = require("commands/saveReplicationDocumentCommand");
import getAutomaticConflictResolutionDocumentCommand = require("commands/getAutomaticConflictResolutionDocumentCommand");
import saveAutomaticConflictResolutionDocumentCommand = require("commands/saveAutomaticConflictResolutionDocumentCommand");
import replicateAllIndexesCommand = require("commands/replicateAllIndexesCommand");
import replicateAllTransformersCommand = require("commands/replicateAllTransformersCommand");
import replicateIndexesCommand = require("commands/replicateIndexesCommand");
import replicateTransformersCommand = require("commands/replicateTransformersCommand");
import appUrl = require("common/appUrl");

class globalConfigReplications extends viewModelBase {

    replicationConfig = ko.observable<replicationConfig>(new replicationConfig({ DocumentConflictResolution: "None", AttachmentConflictResolution: "None" }));
    replicationsSetup = ko.observable<replicationsSetup>(new replicationsSetup({ MergedDocument: { Destinations: [], Source: null } }));

    replicationConfigDirtyFlag = new ko.DirtyFlag([]);
    replicationsSetupDirtyFlag = new ko.DirtyFlag([]);
    
    isConfigSaveEnabled: KnockoutComputed<boolean>;
    isSetupSaveEnabled: KnockoutComputed<boolean>;
    isReplicateIndexesToAllEnabled : KnockoutComputed<boolean>;

    readFromAllAllowWriteToSecondaries = ko.computed(() => {
        var behaviour = this.replicationsSetup().clientFailoverBehaviour();
        if (behaviour == null) {
            return false;
        }
        var tokens = behaviour.split(",");
        return tokens.contains('ReadFromAllServers') && tokens.contains('AllowReadsFromSecondariesAndWritesToSecondaries');
    });

    //TODO: do we need auto conflict resolution in here?
    canActivate(args: any): JQueryPromise<any> {
        var deferred = $.Deferred();
        var db = appUrl.getSystemDatabase();
        if (db) {
            $.when(this.fetchAutomaticConflictResolution(db), this.fetchReplications(db))
                .done(() => deferred.resolve({ can: true }) )
                .fail(() => deferred.resolve({ redirect: appUrl.forSettings(db) }));
        }
        return deferred;
    }

    activate(args) {
        super.activate(args);
        
        this.replicationConfigDirtyFlag = new ko.DirtyFlag([this.replicationConfig]);
        this.isConfigSaveEnabled = ko.computed(() => this.replicationConfigDirtyFlag().isDirty());
        this.replicationsSetupDirtyFlag = new ko.DirtyFlag([this.replicationsSetup, this.replicationsSetup().destinations(), this.replicationConfig, this.replicationsSetup().clientFailoverBehaviour]);
        this.isSetupSaveEnabled = ko.computed(() => this.replicationsSetupDirtyFlag().isDirty());

        this.isReplicateIndexesToAllEnabled = ko.computed(() => this.replicationsSetup().destinations().length > 0);
        var combinedFlag = ko.computed(() => {
            return (this.replicationConfigDirtyFlag().isDirty() || this.replicationsSetupDirtyFlag().isDirty());
        });
        this.dirtyFlag = new ko.DirtyFlag([combinedFlag]);
    }

    fetchAutomaticConflictResolution(db): JQueryPromise<any> {
        var deferred = $.Deferred();
        new getAutomaticConflictResolutionDocumentCommand(db, true)
            .execute()
            .done(repConfig => this.replicationConfig(new replicationConfig(repConfig)))
            .always(() => deferred.resolve({ can: true }));
        return deferred;
    }

    fetchReplications(db): JQueryPromise<any> {
        var deferred = $.Deferred();
        new getGlobalConfigReplicationsCommand(db)
            .execute()
            .done((repSetup: replicationsDto) => this.replicationsSetup(new replicationsSetup({ MergedDocument: repSetup })))
            .always(() => deferred.resolve({ can: true }));
        return deferred;
    }

    createNewDestination() {
        var db = appUrl.getSystemDatabase();
        this.replicationsSetup().destinations.unshift(replicationDestination.empty(db.name));
    }

    removeDestination(repl: replicationDestination) {
        this.replicationsSetup().destinations.remove(repl);
    }

    saveChanges() {
        if (this.isConfigSaveEnabled())
            this.saveAutomaticConflictResolutionSettings();
        if (this.isSetupSaveEnabled()) {
            if (this.replicationsSetup().source()) {
                this.saveReplicationSetup();
            } else {
                var db = appUrl.getSystemDatabase();
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
        var db = appUrl.getSystemDatabase();
        if (db) {
            new saveReplicationDocumentCommand(this.replicationsSetup().toDto(), db, true)
                .execute()
                .done(() => this.replicationsSetupDirtyFlag().reset() );
        }
    }

    saveAutomaticConflictResolutionSettings() {
        var db = appUrl.getSystemDatabase();
        if (db) {
            new saveAutomaticConflictResolutionDocumentCommand(this.replicationConfig().toDto(), db, true)
                .execute()
                .done(() => this.replicationConfigDirtyFlag().reset() );
        }
    }
}

export = globalConfigReplications; 
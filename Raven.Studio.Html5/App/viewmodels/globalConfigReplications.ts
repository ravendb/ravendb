import deleteDocumentCommand = require("commands/deleteDocumentCommand");
import viewModelBase = require("viewmodels/viewModelBase");
import replicationsSetup = require("models/replicationsSetup");
import replicationConfig = require("models/replicationConfig")
import replicationDestination = require("models/replicationDestination");
import getDatabaseStatsCommand = require("commands/getDatabaseStatsCommand");
import getGlobalConfigReplicationsCommand = require("commands/getGlobalConfigReplicationsCommand");
import saveReplicationDocumentCommand = require("commands/saveReplicationDocumentCommand");
import getAutomaticConflictResolutionDocumentCommand = require("commands/getAutomaticConflictResolutionDocumentCommand");
import saveAutomaticConflictResolutionDocumentCommand = require("commands/saveAutomaticConflictResolutionDocumentCommand");
import appUrl = require("common/appUrl");
import messagePublisher = require("common/messagePublisher");

class globalConfigReplications extends viewModelBase {

    replicationConfig = ko.observable<replicationConfig>(new replicationConfig({ DocumentConflictResolution: "None", AttachmentConflictResolution: "None" }));
    replicationsSetup = ko.observable<replicationsSetup>(new replicationsSetup({ MergedDocument: { Destinations: [], Source: null } }));

    replicationConfigDirtyFlag = new ko.DirtyFlag([]);
    replicationsSetupDirtyFlag = new ko.DirtyFlag([]);
    
    isConfigSaveEnabled: KnockoutComputed<boolean>;
    isSetupSaveEnabled: KnockoutComputed<boolean>;

    activated = ko.observable<boolean>(false);

    readFromAllAllowWriteToSecondaries = ko.computed(() => {
        var behaviour = this.replicationsSetup().clientFailoverBehaviour();
        if (behaviour == null) {
            return false;
        }
        var tokens = behaviour.split(",");
        return tokens.contains("ReadFromAllServers") && tokens.contains("AllowReadsFromSecondariesAndWritesToSecondaries");
    });

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

    attached() {
        this.bindPopover();
    }

    bindPopover() {
        $(".dbNameHint").popover({
            html: true,
            container: $("body"),
            trigger: "hover",
            content: "Database name will be replaced with database name being replicated in local configuration."
        });
    }

    activate(args) {
        super.activate(args);
        
        this.replicationConfigDirtyFlag = new ko.DirtyFlag([this.replicationConfig]);
        this.isConfigSaveEnabled = ko.computed(() => this.replicationConfigDirtyFlag().isDirty());
        this.replicationsSetupDirtyFlag = new ko.DirtyFlag([this.replicationsSetup, this.replicationsSetup().destinations(), this.replicationConfig, this.replicationsSetup().clientFailoverBehaviour]);
        this.isSetupSaveEnabled = ko.computed(() => this.replicationsSetupDirtyFlag().isDirty());

        var combinedFlag = ko.computed(() => {
            var f1 = this.replicationConfigDirtyFlag().isDirty();
            var f2 = this.replicationsSetupDirtyFlag().isDirty();
            return f1 || f2;
        });
        this.dirtyFlag = new ko.DirtyFlag([combinedFlag]);
    }

    fetchAutomaticConflictResolution(db): JQueryPromise<any> {
        var deferred = $.Deferred();
        new getAutomaticConflictResolutionDocumentCommand(db, true)
            .execute()
            .done(repConfig => {
                this.replicationConfig(new replicationConfig(repConfig));
                this.activated(true);
            })
            .always(() => deferred.resolve({ can: true }));
        return deferred;
    }

    fetchReplications(db): JQueryPromise<any> {
        var deferred = $.Deferred();
        new getGlobalConfigReplicationsCommand(db)
            .execute()
            .done((repSetup: replicationsDto) => {
                this.replicationsSetup(new replicationsSetup({ MergedDocument: repSetup }));
                this.replicationsSetup().destinations().forEach(d => {
                    d.hasLocal(true);
                    d.hasGlobal(false);
                });
                this.activated(true);
            })
            .always(() => deferred.resolve({ can: true }));
        return deferred;
    }

    createNewDestination() {
        this.replicationsSetup().destinations.unshift(replicationDestination.empty("{databaseName}"));
        this.bindPopover();
    }

    removeDestination(repl: replicationDestination) {
        this.replicationsSetup().destinations.remove(repl);
    }

    saveChanges() {
        this.syncChanges(false);
    }

    syncChanges(deleteConfig: boolean) {
        if (deleteConfig) {
            var task1 = new deleteDocumentCommand("Raven/Global/Replication/Config", appUrl.getSystemDatabase())
                .execute();
            var task2 = new deleteDocumentCommand("Raven/Global/Replication/Destinations", appUrl.getSystemDatabase())
                .execute();
            var combinedTask = $.when(task1, task2);
            combinedTask.done(() => messagePublisher.reportSuccess("Global Settings were successfully saved!"));
            combinedTask.fail((response: JQueryXHR) => messagePublisher.reportError("Failed to save global settings!", response.responseText, response.statusText));

            this.resetUIToDefaultState();

        } else { 
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
    }

    private prepareAndSaveReplicationSetup(source: string) {
        this.replicationsSetup().source(source);
        this.saveReplicationSetup();
    }

    private saveReplicationSetup() {
        var db = appUrl.getSystemDatabase();
        if (db) {
            new saveReplicationDocumentCommand(this.replicationsSetup().toDto(false), db, true)
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

    activateConfig() {
        this.activated(true);
    }

    disactivateConfig() {
        this.confirmationMessage("Delete global configuration for replication?", "Are you sure?")
            .done(() => {
                this.activated(false);
                this.syncChanges(true);
            });
    }

    resetUIToDefaultState() {
        this.replicationConfig().clear();

        var source = this.replicationsSetup().source();
        this.replicationsSetup().clear();
        this.replicationsSetup().source(source);

        this.replicationConfigDirtyFlag().reset();
        this.replicationsSetupDirtyFlag().reset();
    }
}

export = globalConfigReplications; 
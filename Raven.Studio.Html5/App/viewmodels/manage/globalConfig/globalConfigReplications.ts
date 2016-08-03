import deleteDocumentCommand = require("commands/database/documents/deleteDocumentCommand");
import viewModelBase = require("viewmodels/viewModelBase");
import replicationsSetup = require("models/database/replication/replicationsSetup");
import replicationConfig = require("models/database/replication/replicationConfig")
import replicationDestination = require("models/database/replication/replicationDestination");
import getDatabaseStatsCommand = require("commands/resources/getDatabaseStatsCommand");
import getGlobalConfigReplicationsCommand = require("commands/database/globalConfig/getGlobalConfigReplicationsCommand");
import saveReplicationDocumentCommand = require("commands/database/replication/saveReplicationDocumentCommand");
import getAutomaticConflictResolutionDocumentCommand = require("commands/database/replication/getAutomaticConflictResolutionDocumentCommand");
import saveAutomaticConflictResolutionDocumentCommand = require("commands/database/replication/saveAutomaticConflictResolutionDocumentCommand");
import appUrl = require("common/appUrl");
import messagePublisher = require("common/messagePublisher");
import globalConfig = require("viewmodels/manage/globalConfig/globalConfig");
import settingsAccessAuthorizer = require("common/settingsAccessAuthorizer");
import shell = require("viewmodels/shell");

class globalConfigReplications extends viewModelBase {

    developerLicense = globalConfig.developerLicense;
    canUseGlobalConfigurations = globalConfig.canUseGlobalConfigurations;

    replicationConfig = ko.observable<replicationConfig>(new replicationConfig({ DocumentConflictResolution: "None", AttachmentConflictResolution: "None" }));
    replicationsSetup = ko.observable<replicationsSetup>(new replicationsSetup({ MergedDocument: { Destinations: [], Source: null } }));

    replicationConfigDirtyFlag = new ko.DirtyFlag([]);
    replicationsSetupDirtyFlag = new ko.DirtyFlag([]);
    
    isConfigSaveEnabled: KnockoutComputed<boolean>;
    isSetupSaveEnabled: KnockoutComputed<boolean>;

    settingsAccess = new settingsAccessAuthorizer();

    isInCluster = shell.clusterMode;

    activated = ko.observable<boolean>(false);

    skipIndexReplicationForAllDestinationsStatus = ko.observable<string>();

    skipIndexReplicationForAll = ko.observable<boolean>();

    private skipIndexReplicationForAllSubscription: KnockoutSubscription;

    private refereshSkipIndexReplicationForAllDestinations() {
        if (this.skipIndexReplicationForAllSubscription != null)
            this.skipIndexReplicationForAllSubscription.dispose();

        var newStatus = this.getIndexReplicationStatusForAllDestinations();
        this.skipIndexReplicationForAll(newStatus === 'all');

        this.skipIndexReplicationForAllSubscription = this.skipIndexReplicationForAll.subscribe(newValue => this.toggleIndexReplication(newValue));
    }

    private getIndexReplicationStatusForAllDestinations(): string {
        var countOfSkipIndexReplication: number = 0;
        ko.utils.arrayForEach(this.replicationsSetup().destinations(), dest => {
            if (dest.skipIndexReplication()) {
                countOfSkipIndexReplication++;
            }
        });

        // ReSharper disable once ConditionIsAlwaysConst
        if (countOfSkipIndexReplication === 0)
            return 'none';

        if (countOfSkipIndexReplication === this.replicationsSetup().destinations().length)
            return 'all';

        return 'mixed';
    }

    canActivate(args: any): JQueryPromise<any> {
        var deferred = $.Deferred();
        var db = appUrl.getSystemDatabase();
        if (db) {
            if (this.settingsAccess.isForbidden()) {
                deferred.resolve({ can: true });
            } else {
                $.when(this.fetchAutomaticConflictResolution(db), this.fetchReplications(db))
                    .done(() => deferred.resolve({ can: true }))
                    .fail(() => deferred.resolve({ redirect: appUrl.forSettings(db) }));
            }
        }
        return deferred;
    }

    attached() {
        super.attached();
        this.bindPopover();
        this.refereshSkipIndexReplicationForAllDestinations();
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
        this.replicationsSetupDirtyFlag = new ko.DirtyFlag([this.replicationsSetup, this.replicationsSetup().destinations(), this.replicationConfig, this.replicationsSetup().clientFailoverBehaviour, this.replicationsSetup().requestTimeSlaThreshold, this.replicationsSetup().showRequestTimeSlaThreshold]);
        this.isSetupSaveEnabled = ko.computed(() => !this.settingsAccess.isReadOnly() && this.replicationsSetupDirtyFlag().isDirty());

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

        ko.postbox.subscribe('skip-index-replication', () => this.refereshSkipIndexReplicationForAllDestinations());

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
        this.refereshSkipIndexReplicationForAllDestinations();
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

    toggleIndexReplication(skipReplicationValue: boolean) {
        this.replicationsSetup().destinations().forEach(dest => {
            dest.skipIndexReplication(skipReplicationValue);
        });
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

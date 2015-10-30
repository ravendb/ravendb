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
import replicateAllIndexesCommand = require("commands/replicateAllIndexesCommand");
import replicateAllTransformersCommand = require("commands/replicateAllTransformersCommand");
import replicateIndexesCommand = require("commands/replicateIndexesCommand");
import replicateTransformersCommand = require("commands/replicateTransformersCommand");
import appUrl = require("common/appUrl");

class replications extends viewModelBase {

    prefixForHilo = ko.observable<string>('');
    replicationConfig = ko.observable<replicationConfig>(new replicationConfig({ DocumentConflictResolution: "None", AttachmentConflictResolution: "None" }));
    replicationsSetup = ko.observable<replicationsSetup>(new replicationsSetup({ Destinations: [], Source: null }));


    serverPrefixForHiLoDirtyFlag = new ko.DirtyFlag([]);
    replicationConfigDirtyFlag = new ko.DirtyFlag([]);
    replicationsSetupDirtyFlag = new ko.DirtyFlag([]);
    
    isServerPrefixForHiLoSaveEnabled: KnockoutComputed<boolean>;
    isConfigSaveEnabled: KnockoutComputed<boolean>;
    isSetupSaveEnabled: KnockoutComputed<boolean>;

    skipIndexReplicationForAllDestinationsStatus = ko.observable<string>();

    skipIndexReplicationForAll = ko.observable<boolean>();
    
    private skipIndexReplicationForAllSubscription : KnockoutSubscription;

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
            if (dest.skipIndexReplication() === true) {
                countOfSkipIndexReplication++;
            }
        });

        if (countOfSkipIndexReplication === this.replicationsSetup().destinations().length)
            return 'all';

        // ReSharper disable once ConditionIsAlwaysConst
        if (countOfSkipIndexReplication === 0)
            return 'none';

        return 'mixed';
    }

    readFromAllAllowWriteToSecondaries = ko.computed(() => {
        var behaviour = this.replicationsSetup().clientFailoverBehaviour();
        if (behaviour == null) {
            return false;
        }
        var tokens = behaviour.split(",");
        return tokens.contains('ReadFromAllServers') && tokens.contains('AllowReadsFromSecondariesAndWritesToSecondaries');
    });

    canActivate(args: any): JQueryPromise<any> {
        var deferred = $.Deferred();
        var db = this.activeDatabase();
        if (db) {
            $.when(this.fetchServerPrefixForHiLoCommand(db), this.fetchAutomaticConflictResolution(db), this.fetchReplications(db))
                .done(() => deferred.resolve({ can: true }) )
                .fail(() => deferred.resolve({ redirect: appUrl.forSettings(db) }));
        }
        return deferred;
    }

    activate(args) {
        super.activate(args);
        this.updateHelpLink('7K1KES');

        this.serverPrefixForHiLoDirtyFlag = new ko.DirtyFlag([this.prefixForHilo]);
        this.isServerPrefixForHiLoSaveEnabled = ko.computed(() => this.serverPrefixForHiLoDirtyFlag().isDirty());
        this.replicationConfigDirtyFlag = new ko.DirtyFlag([this.replicationConfig]);
        this.isConfigSaveEnabled = ko.computed(() => this.replicationConfigDirtyFlag().isDirty());
        this.replicationsSetupDirtyFlag = new ko.DirtyFlag([this.replicationsSetup, this.replicationsSetup().destinations(), this.skipIndexReplicationForAll, this.replicationConfig, this.replicationsSetup().clientFailoverBehaviour]);
        this.isSetupSaveEnabled = ko.computed(() => this.replicationsSetupDirtyFlag().isDirty());

        var combinedFlag = ko.computed(() => {
            return (this.replicationConfigDirtyFlag().isDirty() || this.replicationsSetupDirtyFlag().isDirty() || this.serverPrefixForHiLoDirtyFlag().isDirty());
        });
        this.dirtyFlag = new ko.DirtyFlag([combinedFlag]);
    }

    private fetchServerPrefixForHiLoCommand(db): JQueryPromise<any> {
        var deferred = $.Deferred();
        new getServerPrefixForHiLoCommand(db)
            .execute()
            .done((result) => this.prefixForHilo(result))
            .always(() => deferred.resolve({ can: true }));
        return deferred;
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
            .done(repSetup =>
            {
                this.replicationsSetup(new replicationsSetup(repSetup));
                              
                ko.postbox.subscribe('skip-index-replication',() => this.refereshSkipIndexReplicationForAllDestinations());

                var status = this.getIndexReplicationStatusForAllDestinations();
                if (status === 'all')
                    this.skipIndexReplicationForAll(true);
                else
                    this.skipIndexReplicationForAll(false);    

                this.skipIndexReplicationForAllSubscription = this.skipIndexReplicationForAll.subscribe(newValue => this.toggleIndexReplication(newValue));
            })
            .always(() => deferred.resolve({ can: true }));
        return deferred;
    }

    toggleSkipIndexReplicationForAll() {
        this.skipIndexReplicationForAll.toggle();
    }

    createNewDestination() {
        var db = this.activeDatabase();
        this.replicationsSetup().destinations.unshift(replicationDestination.empty(db.name));
        this.refereshSkipIndexReplicationForAllDestinations();
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

    toggleIndexReplication(skipReplicationValue : boolean) {
        this.replicationsSetup().destinations().forEach(dest => {
            dest.skipIndexReplication(skipReplicationValue);
        });
    }

    sendReplicateCommand(destination: replicationDestination, parentClass: replications) {        
        var db = parentClass.activeDatabase();
        if (db) {
            new replicateIndexesCommand(db, destination).execute();
            new replicateTransformersCommand(db, destination).execute();
        } else {
            alert('No database selected! This error should not be seen.'); //precaution to ease debugging - in case something bad happens
        }        
    }

    sendReplicateAllCommand() {
        var db = this.activeDatabase();
        if (db) {
            new replicateAllIndexesCommand(db).execute();
            new replicateAllTransformersCommand(db).execute();
        } else {
            alert('No database selected! This error should not be seen.'); //precaution to ease debugging - in case something bad happens
        }
    }

    saveServerPrefixForHiLo() {
        var db = this.activeDatabase();
        if (db) {
            new updateServerPrefixHiLoCommand(this.prefixForHilo(), db)
                .execute()
                .done(() => this.serverPrefixForHiLoDirtyFlag().reset());
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

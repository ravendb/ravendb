import viewModelBase = require("viewmodels/viewModelBase");
import replicationsSetup = require("models/replicationsSetup");
import replicationConfig = require("models/replicationConfig")
import replicationDestination = require("models/replicationDestination");
import getDatabaseStatsCommand = require("commands/getDatabaseStatsCommand");
import getReplicationsCommand = require("commands/getReplicationsCommand");
import updateServerPrefixHiLoCommand = require("commands/updateServerPrefixHiLoCommand");
import saveReplicationDocumentCommand = require("commands/saveReplicationDocumentCommand");
import saveAutomaticConflictResolutionDocument = require("commands/saveAutomaticConflictResolutionDocument");
import getServerPrefixForHiLoCommand = require("commands/getServerPrefixForHiLoCommand");
import replicateAllIndexesCommand = require("commands/replicateAllIndexesCommand");
import replicateAllTransformersCommand = require("commands/replicateAllTransformersCommand");
import deleteLocalReplicationsSetupCommand = require("commands/deleteLocalReplicationsSetupCommand");
import replicateIndexesCommand = require("commands/replicateIndexesCommand");
import replicateTransformersCommand = require("commands/replicateTransformersCommand");
import getEffectiveConflictResolutionCommand = require("commands/getEffectiveConflictResolutionCommand");
import appUrl = require("common/appUrl");

class replications extends viewModelBase {

    prefixForHilo = ko.observable<string>("");
    replicationConfig = ko.observable<replicationConfig>(new replicationConfig({ DocumentConflictResolution: "None", AttachmentConflictResolution: "None" }));
    replicationsSetup = ko.observable<replicationsSetup>(new replicationsSetup({ MergedDocument: { Destinations: [], Source: null } }));
    globalClientFailoverBehaviour = ko.observable<string>(null);
    globalReplicationConfig = ko.observable<replicationConfig>();

    serverPrefixForHiLoDirtyFlag = new ko.DirtyFlag([]);
    replicationConfigDirtyFlag = new ko.DirtyFlag([]);
    replicationsSetupDirtyFlag = new ko.DirtyFlag([]);

    isServerPrefixForHiLoSaveEnabled: KnockoutComputed<boolean>;
    isConfigSaveEnabled: KnockoutComputed<boolean>;
    isSetupSaveEnabled: KnockoutComputed<boolean>;
    isReplicateIndexesToAllEnabled: KnockoutComputed<boolean>;

    usingGlobal = ko.observable<boolean>(false);
    hasGlobalValues = ko.observable<boolean>(false);

    readFromAllAllowWriteToSecondaries = ko.computed(() => {
        var behaviour = this.replicationsSetup().clientFailoverBehaviour();
        if (behaviour == null) {
            return false;
        }
        var tokens = behaviour.split(",");
        return tokens.contains("ReadFromAllServers") && tokens.contains("AllowReadsFromSecondariesAndWritesToSecondaries");
    });

    globalReadFromAllAllowWriteToSecondaries = ko.computed(() => {
        var behaviour = this.globalClientFailoverBehaviour();
        if (behaviour == null) {
            return false;
        }
        var tokens = behaviour.split(",");
        return tokens.contains("ReadFromAllServers") && tokens.contains("AllowReadsFromSecondariesAndWritesToSecondaries");
    });

    canActivate(args: any): JQueryPromise<any> {
        var deferred = $.Deferred();
        var db = this.activeDatabase();
        if (db) {
            $.when(this.fetchServerPrefixForHiLoCommand(db), this.fetchAutomaticConflictResolution(db), this.fetchReplications(db))
                .done(() => deferred.resolve({ can: true }))
                .fail(() => deferred.resolve({ redirect: appUrl.forSettings(db) }));
        }
        return deferred;
    }

    activate(args) {
        super.activate(args);
        this.updateHelpLink("7K1KES");

        this.serverPrefixForHiLoDirtyFlag = new ko.DirtyFlag([this.prefixForHilo]);
        this.isServerPrefixForHiLoSaveEnabled = ko.computed(() => this.serverPrefixForHiLoDirtyFlag().isDirty());
        this.replicationConfigDirtyFlag = new ko.DirtyFlag([this.replicationConfig]);
        this.isConfigSaveEnabled = ko.computed(() => this.replicationConfigDirtyFlag().isDirty());
        this.replicationsSetupDirtyFlag = new ko.DirtyFlag([this.replicationsSetup, this.replicationsSetup().destinations(), this.replicationConfig, this.replicationsSetup().clientFailoverBehaviour, this.usingGlobal]);
        this.isSetupSaveEnabled = ko.computed(() => this.replicationsSetupDirtyFlag().isDirty());

        this.isReplicateIndexesToAllEnabled = ko.computed(() => this.replicationsSetup().destinations().length > 0);
        var combinedFlag = ko.computed(() => {
            var rc = this.replicationConfigDirtyFlag().isDirty();
            var rs = this.replicationsSetupDirtyFlag().isDirty();
            var sp = this.serverPrefixForHiLoDirtyFlag().isDirty();
            return rc || rs || sp;
        });
        this.dirtyFlag = new ko.DirtyFlag([combinedFlag, this.usingGlobal]);
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
        new getEffectiveConflictResolutionCommand(db)
            .execute()
            .done((repConfig: configurationDocumentDto<replicationConfigDto>) => {
                this.replicationConfig(new replicationConfig(repConfig.MergedDocument));
                if (repConfig.GlobalDocument) {
                    this.globalReplicationConfig(new replicationConfig(repConfig.GlobalDocument));
                }
            })
            .always(() => deferred.resolve({ can: true }));
        return deferred;
    }

    fetchReplications(db): JQueryPromise<any> {
        var deferred = $.Deferred();
        new getReplicationsCommand(db)
            .execute()
            .done((repSetup: configurationDocumentDto<replicationsDto>) => {
                this.replicationsSetup(new replicationsSetup(repSetup));
                this.usingGlobal(repSetup.GlobalExists && !repSetup.LocalExists);
                this.hasGlobalValues(repSetup.GlobalExists);
                if (repSetup.GlobalDocument && repSetup.GlobalDocument.ClientConfiguration) {
                    this.globalClientFailoverBehaviour(repSetup.GlobalDocument.ClientConfiguration.FailoverBehavior);
                }
            })
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

    saveChanges() {
        if (this.usingGlobal()) {
            new deleteLocalReplicationsSetupCommand(this.activeDatabase())
                .execute();
        } else {
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
                .done(() => this.replicationsSetupDirtyFlag().reset());
        }
    }

    sendReplicateCommand(destination: replicationDestination, parentClass: replications) {
        var db = parentClass.activeDatabase();
        if (db) {
            new replicateIndexesCommand(db, destination).execute();
            new replicateTransformersCommand(db, destination).execute();
        } else {
            alert("No database selected! This error should not be seen."); //precaution to ease debugging - in case something bad happens
        }
    }

    sendReplicateAllCommand() {
        var db = this.activeDatabase();
        if (db) {
            new replicateAllIndexesCommand(db).execute();
            new replicateAllTransformersCommand(db).execute();
        } else {
            alert("No database selected! This error should not be seen."); //precaution to ease debugging - in case something bad happens
        }

    }

    saveServerPrefixForHiLo() {
        var db = this.activeDatabase();
        if (db) {
            new updateServerPrefixHiLoCommand(this.prefixForHilo(), db)
                .execute()
                .done(() => {
                    this.serverPrefixForHiLoDirtyFlag().reset();
                    this.dirtyFlag().reset();
                });
        }
    }

    saveAutomaticConflictResolutionSettings() {
        var db = this.activeDatabase();
        if (db) {
            new saveAutomaticConflictResolutionDocument(this.replicationConfig().toDto(), db)
                .execute()
                .done(() => {
                    this.replicationConfigDirtyFlag().reset();
                    this.dirtyFlag().reset();
                });
        }
    }

    override(value: boolean, destination: replicationDestination) {
        destination.hasLocal(value);
        if (!destination.hasLocal()) {
            destination.copyFromGlobal();
        }
    }

    useLocal() {
        this.usingGlobal(false);
    }

    useGlobal() {
        this.usingGlobal(true);
        if (this.globalReplicationConfig()) {
            this.replicationConfig().attachmentConflictResolution(this.globalReplicationConfig().attachmentConflictResolution());
            this.replicationConfig().documentConflictResolution(this.globalReplicationConfig().documentConflictResolution());    
        }
        
        this.replicationsSetup().copyFromParent(this.globalClientFailoverBehaviour());
    }
}

export = replications; 
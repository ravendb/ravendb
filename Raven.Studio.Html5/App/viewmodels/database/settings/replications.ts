import viewModelBase = require("viewmodels/viewModelBase");
import replicationsSetup = require("models/database/replication/replicationsSetup");
import replicationConfig = require("models/database/replication/replicationConfig")
import replicationDestination = require("models/database/replication/replicationDestination");
import getReplicationsCommand = require("commands/database/replication/getReplicationsCommand");
import updateServerPrefixHiLoCommand = require("commands/database/documents/updateServerPrefixHiLoCommand");
import saveReplicationDocumentCommand = require("commands/database/replication/saveReplicationDocumentCommand");
import saveAutomaticConflictResolutionDocument = require("commands/database/replication/saveAutomaticConflictResolutionDocument");
import getServerPrefixForHiLoCommand = require("commands/database/documents/getServerPrefixForHiLoCommand");
import replicateAllIndexesCommand = require("commands/database/replication/replicateAllIndexesCommand");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import replicateAllTransformersCommand = require("commands/database/replication/replicateAllTransformersCommand");
import deleteLocalReplicationsSetupCommand = require("commands/database/globalConfig/deleteLocalReplicationsSetupCommand");
import replicateIndexesCommand = require("commands/database/replication/replicateIndexesCommand");
import replicateTransformersCommand = require("commands/database/replication/replicateTransformersCommand");
import getEffectiveConflictResolutionCommand = require("commands/database/globalConfig/getEffectiveConflictResolutionCommand");
import appUrl = require("common/appUrl");
import enableReplicationCommand = require("commands/database/replication/enableReplicationCommand");
import resolveAllConflictsCommand = require("commands/database/replication/resolveAllConflictsCommand");
import database = require("models/resources/database");

class replications extends viewModelBase {

    replicationEnabled = ko.observable<boolean>(false);

    prefixForHilo = ko.observable<string>("");
    replicationConfig = ko.observable<replicationConfig>(new replicationConfig({ DocumentConflictResolution: "None", AttachmentConflictResolution: "None" }));
    replicationsSetup = ko.observable<replicationsSetup>(new replicationsSetup({ MergedDocument: { Destinations: [], Source: null } }));
    globalClientFailoverBehaviour = ko.observable<string>(null);
    globalClientRequestTimeSlaThreshold = ko.observable<number>();
    globalReplicationConfig = ko.observable<replicationConfig>();

    serverPrefixForHiLoDirtyFlag = new ko.DirtyFlag([]);
    replicationConfigDirtyFlag = new ko.DirtyFlag([]);
    replicationsSetupDirtyFlag = new ko.DirtyFlag([]);

    isServerPrefixForHiLoSaveEnabled: KnockoutComputed<boolean>;
    isConfigSaveEnabled: KnockoutComputed<boolean>;
    isSetupSaveEnabled: KnockoutComputed<boolean>;

    skipIndexReplicationForAllDestinationsStatus = ko.observable<string>();
    skipIndexReplicationForAll = ko.observable<boolean>();

    showRequestTimeoutRow: KnockoutComputed<boolean>;

    private skipIndexReplicationForAllSubscription: KnockoutSubscription;

    private refereshSkipIndexReplicationForAllDestinations() {
        if (this.skipIndexReplicationForAllSubscription != null)
            this.skipIndexReplicationForAllSubscription.dispose();

        var newStatus = this.getIndexReplicationStatusForAllDestinations();
        this.skipIndexReplicationForAll(newStatus === 'all');

        this.skipIndexReplicationForAllSubscription = this.skipIndexReplicationForAll.subscribe(newValue => this.toggleIndexReplication(newValue));
    }

    private getIndexReplicationStatusForAllDestinations(): string {
        var nonEtlDestinations = this.replicationsSetup().destinations().filter(x => !x.enableReplicateOnlyFromCollections());
        var nonEtlWithSkipedIndexReplicationCount = nonEtlDestinations.filter(x => x.skipIndexReplication()).length;

        if (nonEtlWithSkipedIndexReplicationCount === 0)
            return 'none';

        if (nonEtlWithSkipedIndexReplicationCount === nonEtlDestinations.length)
            return 'all';

        return 'mixed';
    }

    usingGlobal = ko.observable<boolean>(false);
    hasGlobalValues = ko.observable<boolean>(false);

    globalReadFromAllAllowWriteToSecondaries = ko.computed(() => {
        var behaviour = this.globalClientFailoverBehaviour();
        if (behaviour == null) {
            return false;
        }
        var tokens = behaviour.split(",").map(x => x.trim());
        return tokens.contains("ReadFromAllServers") && tokens.contains("AllowReadsFromSecondariesAndWritesToSecondaries");
    });

    globalReadFromAllButSwitchWhenRequestTimeSlaThresholdIsReached = ko.computed(() => {
        var behaviour = this.globalClientFailoverBehaviour();
        if (behaviour == null) {
            return false;
        }
        var tokens = behaviour.split(",").map(x => x.trim());
        return tokens.contains("ReadFromAllServers") && tokens.contains("AllowReadFromSecondariesWhenRequestTimeSlaThresholdIsReached");
    });

    constructor() {
        super();
        aceEditorBindingHandler.install();

        this.showRequestTimeoutRow = ko.computed(() => {
            var localSetting = this.replicationsSetup().showRequestTimeSlaThreshold();
            var globalSetting = this.hasGlobalValues() && this.globalClientFailoverBehaviour() &&
                this.globalClientFailoverBehaviour().contains("AllowReadFromSecondariesWhenRequestTimeSlaThresholdIsReached");
            return localSetting || globalSetting;
        });
    }

    canActivate(args: any): JQueryPromise<any> {
        var deferred = $.Deferred();
        var db = this.activeDatabase();
        if (db) {
            if (db.activeBundles.contains("Replication")) {
                this.replicationEnabled(true);
                $.when(this.fetchServerPrefixForHiLoCommand(db), this.fetchAutomaticConflictResolution(db), this.fetchReplications(db))
                    .done(() => deferred.resolve({ can: true }))
                    .fail(() => deferred.resolve({ redirect: appUrl.forSettings(db) }));
            } else {
                this.replicationEnabled(false);
                deferred.resolve({ can: true });
            }

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

        var replicationSetupDirtyFlagItems = [this.replicationsSetup, this.replicationsSetup().destinations(), this.skipIndexReplicationForAll, this.replicationConfig, this.replicationsSetup().clientFailoverBehaviour, this.usingGlobal];

        this.replicationsSetupDirtyFlag = new ko.DirtyFlag(replicationSetupDirtyFlagItems);

        this.isSetupSaveEnabled = ko.computed(() => this.replicationsSetupDirtyFlag().isDirty());

        var combinedFlag = ko.computed(() => {
            var rc = this.replicationConfigDirtyFlag().isDirty();
            var rs = this.replicationsSetupDirtyFlag().isDirty();
            var sp = this.serverPrefixForHiLoDirtyFlag().isDirty();
            return rc || rs || sp;
        });
        this.dirtyFlag = new ko.DirtyFlag([combinedFlag, this.usingGlobal]);
    }

    attached() {
        super.attached();
        $.each(this.replicationsSetup().destinations(), this.addScriptHelpPopover);
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
        ko.postbox.subscribe('skip-index-replication', () => this.refereshSkipIndexReplicationForAllDestinations());

        new getReplicationsCommand(db)
            .execute()
            .done((repSetup: configurationDocumentDto<replicationsDto>) => {
                this.replicationsSetup(new replicationsSetup(repSetup));
                this.usingGlobal(repSetup.GlobalExists && !repSetup.LocalExists);
                this.hasGlobalValues(repSetup.GlobalExists);
                if (repSetup.GlobalDocument && repSetup.GlobalDocument.ClientConfiguration) {
                    this.globalClientFailoverBehaviour(repSetup.GlobalDocument.ClientConfiguration.FailoverBehavior);
                    this.globalClientRequestTimeSlaThreshold(repSetup.GlobalDocument.ClientConfiguration.RequestTimeSlaThresholdInMilliseconds);
                }

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

    addScriptHelpPopover() {
        $(".scriptPopover").popover({
            html: true,
            trigger: 'hover',
            container: ".form-horizontal",
            content:
            '<p>Return <code>null</code> in transform script to skip document from replication. </p>' +
            '<p>Example: </p>' +
            '<pre><span class="code-keyword">if</span> (<span class="code-keyword">this</span>.Region !== <span class="code-string">"Europe"</span>) { <br />   <span class="code-keyword">return null</span>; <br />}<br/><span class="code-keyword">this</span>.Currency = <span class="code-string">"EUR"</span>; </pre>'
        });
    }

    public onTransformCollectionClick(destination: replicationDestination, collectionName: string) {
        var collections = destination.specifiedCollections();
        var item = collections.first(c => c.collection() === collectionName);

        if (typeof (item.script()) === "undefined") {
            item.script("");
        } else {
            item.script(undefined);
        }

        destination.specifiedCollections.notifySubscribers();
    }

    toggleSkipIndexReplicationForAll() {
        this.skipIndexReplicationForAll.toggle();
    }

    createNewDestination() {
        var db = this.activeDatabase();
        this.replicationsSetup().destinations.unshift(replicationDestination.empty(db.name));
        this.refereshSkipIndexReplicationForAllDestinations();
        this.addScriptHelpPopover();
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
                    var db: database = this.activeDatabase();
                    this.prepareAndSaveReplicationSetup(db.statistics().databaseId());
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
                .done(() => {
                    this.replicationsSetupDirtyFlag().reset();
                    this.dirtyFlag().reset();
                });
        }
    }

    toggleIndexReplication(skipReplicationValue: boolean) {
        this.replicationsSetup().destinations().forEach(dest => {
            // since we are on replications page filter toggle to non-etl destinations only
            if (!dest.enableReplicateOnlyFromCollections()) {
                dest.skipIndexReplication(skipReplicationValue);    
            }
        });
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

    sendResolveAllConflictsCommand() {
        var db = this.activeDatabase();
        if (db) {
            new resolveAllConflictsCommand(db).execute();
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
        // using global configuration will discard all ETL configurations, if you find any warning user about this. 
        if (this.replicationsSetup().destinations().filter(x => x.enableReplicateOnlyFromCollections()).length) {
            this.confirmationMessage("Are you sure?",
                    "All ETL destinations will be discarded when using global configuration.")
                .done(() => this.proceedWithUseGlobal());
        } else {
            this.proceedWithUseGlobal();
        }
    }

    private proceedWithUseGlobal() {
        this.usingGlobal(true);
        if (this.globalReplicationConfig()) {
            this.replicationConfig().attachmentConflictResolution(this.globalReplicationConfig().attachmentConflictResolution());
            this.replicationConfig().documentConflictResolution(this.globalReplicationConfig().documentConflictResolution());
        }

        this.replicationsSetup().copyFromParent(this.globalClientFailoverBehaviour(), this.globalClientRequestTimeSlaThreshold());
    }

    enableReplication() {
        new enableReplicationCommand(this.activeDatabase())
            .execute()
            .done((bundles) => {
                var db = this.activeDatabase();
                db.activeBundles(bundles);
                this.replicationEnabled(true);
                this.fetchServerPrefixForHiLoCommand(db);
                this.fetchAutomaticConflictResolution(db);
                this.fetchReplications(db);
            });
    }

}

export = replications; 

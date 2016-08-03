import appUrl = require("common/appUrl");
import dialog = require("plugins/dialog");
import database = require("models/resources/database");
import createResourceBase = require("viewmodels/resources/createResourceBase");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import getPluginsInfoCommand = require("commands/database/debug/getPluginsInfoCommand");
import getDatabaseStatsCommand = require("commands/resources/getDatabaseStatsCommand");
import getStatusDebugConfigCommand = require("commands/database/debug/getStatusDebugConfigCommand");
import getClusterTopologyCommand = require("commands/database/cluster/getClusterTopologyCommand");
import topology = require("models/database/replication/topology");
import shell = require("viewmodels/shell");

class createDatabase extends createResourceBase {
    resourceNameCapitalString = "Database";
    resourceNameString = "database";

    databaseIndexesPath = ko.observable("");
    indexesCustomValidityError: KnockoutComputed<string>;
    
    isCompressionBundleEnabled = ko.observable(false);
    isEncryptionBundleEnabled = ko.observable(false);
    isExpirationBundleEnabled = ko.observable(false);
    isQuotasBundleEnabled = ko.observable(false);
    isReplicationBundleEnabled = ko.observable(false);
    isSqlReplicationBundleEnabled = ko.observable(false);
    isVersioningBundleEnabled = ko.observable(false);
    isPeriodicExportBundleEnabled = ko.observable(false); // Old Raven Studio has this enabled by default
    isScriptedIndexBundleEnabled = ko.observable(false);
    isIncrementalBackupChecked = ko.observable(false);
    isClusterWideVisible = ko.observable(false);
    isClusterWideChecked = ko.observable(true);
    alertTimeout = ko.observable("");
    alertRecurringTimeout = ko.observable("");

    customBundles = ko.observableArray<string>();
    selectedCustomBundles = ko.observableArray<string>([]);

    replicationBundleChangeDisabled = ko.computed(() => {
        var clusterMode = shell.clusterMode();
        var clusterWide = this.isClusterWideChecked();
        return clusterMode && clusterWide;
    });

    constructor(parent: dialogViewModelBase) {
        super(shell.databases, parent);

        if (!!this.licenseStatus() && this.licenseStatus().IsCommercial && this.licenseStatus().Attributes.periodicBackup !== "true") {
            this.isPeriodicExportBundleEnabled(false);
        }

        if (shell.clusterMode()) {
            this.isReplicationBundleEnabled(true);
        }

        this.indexesCustomValidityError = ko.computed(() => {
            var newPath = this.databaseIndexesPath();
            var errorMessage: string = this.isPathLegal(newPath, "Indexes");
            return errorMessage;
        });

        this.fetchCustomBundles();
        this.fetchAllowVoron();

        this.fetchClusterWideConfig();
    }

    fetchClusterWideConfig() {
        new getClusterTopologyCommand(appUrl.getSystemDatabase())
            .execute()
            .done((topology: topology) => {
                this.isClusterWideVisible(topology && topology.allNodes().length > 0);
            });
    }

    fetchCustomBundles() {
        new getPluginsInfoCommand(appUrl.getSystemDatabase())
            .execute()
            .done((result: pluginsInfoDto) => {
            this.customBundles(result.CustomBundles);
        });
    }

    fetchAllowVoron() {
        $.when(new getDatabaseStatsCommand(appUrl.getSystemDatabase()).execute(),
            new getStatusDebugConfigCommand(appUrl.getSystemDatabase()).execute()
        ).done((stats: Array<databaseStatisticsDto>, config: any) => {
            this.allowVoron(stats[0].Is64Bit || config[0].Storage.Voron.AllowOn32Bits);
        });
    }

    nextOrCreate() {
        this.creationTaskStarted = true;
        dialog.close(this.parent);
        this.creationTask.resolve(this.resourceName(), this.getActiveBundles(), this.resourcePath(), this.logsPath(), this.databaseIndexesPath(), this.resourceTempPath(), this.storageEngine(),
            this.isIncrementalBackupChecked(), this.alertTimeout(), this.alertRecurringTimeout(), this.isClusterWideChecked());
        this.clearResourceName();
    }

    private isDatabaseNameExists(databaseName: string, databases: database[]): boolean {
        databaseName = databaseName.toLowerCase();
        for (var i = 0; i < databases.length; i++) {
            if (databaseName === databases[i].name.toLowerCase()) {
                return true;
            }
        }
        return false;
    }

    toggleCompressionBundle() {
        this.isCompressionBundleEnabled.toggle();
    }

    toggleEncryptionBundle() {
        this.isEncryptionBundleEnabled.toggle();
    }

    toggleExpirationBundle() {
        this.isExpirationBundleEnabled.toggle();
    }

    toggleQuotasBundle() {
        this.isQuotasBundleEnabled.toggle();
    }

    toggleReplicationBundle() {
        this.isReplicationBundleEnabled.toggle();
    }

    toggleSqlReplicationBundle() {
        this.isSqlReplicationBundleEnabled.toggle();
    }

    toggleVersioningBundle() {
        this.isVersioningBundleEnabled.toggle();
    }

    togglePeriodicExportBundle() {
        this.isPeriodicExportBundleEnabled.toggle();
    }

    toggleScriptedIndexBundle() {
        this.isScriptedIndexBundleEnabled.toggle();
    }

    toggleCustomBundle(name: string) {
        if (this.selectedCustomBundles.contains(name)) {
            this.selectedCustomBundles.remove(name);
        } else {
            this.selectedCustomBundles.push(name);
        }
    }

    isCustomBundleEnabled(name: string) {
        return this.selectedCustomBundles().contains(name);
    }

    private getActiveBundles(): string[] {
        var activeBundles: string[] = [];
        if (this.isCompressionBundleEnabled()) {
            activeBundles.push("Compression");
        }

        if (this.isEncryptionBundleEnabled()) {
            activeBundles.push("Encryption");
        }

        if (this.isExpirationBundleEnabled()) {
            activeBundles.push("DocumentExpiration");
        }

        if (this.isQuotasBundleEnabled()) {
            activeBundles.push("Quotas");
        }

        if (this.isReplicationBundleEnabled()) {
            activeBundles.push("Replication"); // TODO: Replication also needs to store 2 documents containing information about replication. See http://ravendb.net/docs/2.5/server/scaling-out/replication?version=2.5
        }

        if (this.isSqlReplicationBundleEnabled()) {
            activeBundles.push("SqlReplication");
        }

        if (this.isVersioningBundleEnabled()) {
            activeBundles.push("Versioning");
        }

        if (this.isPeriodicExportBundleEnabled()) {
            activeBundles.push("PeriodicExport");
        }

        if (this.isScriptedIndexBundleEnabled()) {
            activeBundles.push("ScriptedIndexResults");
        }

        activeBundles.pushAll(this.selectedCustomBundles());

        return activeBundles;
    }
}

export = createDatabase;

import app = require("durandal/app");
import document = require("models/document");
import dialog = require("plugins/dialog");
import createDatabaseCommand = require("commands/createDatabaseCommand");
import collection = require("models/collection");
import viewModelBase = require("viewmodels/viewModelBase");
import database = require("models/database");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class createDatabase extends viewModelBase {

    public creationTask = $.Deferred();
    creationTaskStarted = false;

    databaseName = ko.observable('');
    nameCustomValidityError: KnockoutComputed<string>;
    databasePath = ko.observable('');
    pathCustomValidityError: KnockoutComputed<string>;
    databaseLogsPath = ko.observable('');
    logsCustomValidityError: KnockoutComputed<string>;
    databaseIndexesPath = ko.observable('');
    indexesCustomValidityError: KnockoutComputed<string>;
    databaseTempPath = ko.observable('');
    tempCustomValidityError: KnockoutComputed<string>;
    storageEngine = ko.observable('');
    tempPathVisible = ko.computed(() => "voron" == this.storageEngine());
    private maxNameLength = 200;

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
    alertTimeout = ko.observable("");
    alertRecurringTimeout = ko.observable("");

    constructor(private databases: KnockoutObservableArray<database>, private licenseStatus: KnockoutObservable<licenseStatusDto>, private parent: dialogViewModelBase) {
        super();

        this.licenseStatus = licenseStatus;
        if (!!this.licenseStatus() && this.licenseStatus().IsCommercial && this.licenseStatus().Attributes.periodicBackup !== "true") {
            this.isPeriodicExportBundleEnabled(false);
        }

        this.nameCustomValidityError = ko.computed(() => {
            var errorMessage: string = '';
            var newDatabaseName = this.databaseName();

            if (this.isDatabaseNameExists(newDatabaseName, this.databases()) == true) {
                errorMessage = "Database name already exists!";
            }
            else if ((errorMessage = this.checkName(newDatabaseName)) != '') { }

            return errorMessage;
        });

        this.pathCustomValidityError = ko.computed(() => {
            var newPath = this.databasePath();
            var errorMessage: string = this.isPathLegal(newPath, "Path");
            return errorMessage;
        });

        this.logsCustomValidityError = ko.computed(() => {
            var newPath = this.databaseLogsPath();
            var errorMessage: string = this.isPathLegal(newPath, "Logs");
            return errorMessage;
        });

        this.indexesCustomValidityError = ko.computed(() => {
            var newPath = this.databaseIndexesPath();
            var errorMessage: string = this.isPathLegal(newPath, "Indexes");
            return errorMessage;
        });

        this.tempCustomValidityError = ko.computed(() => {
            var newPath = this.databaseIndexesPath();
            var errorMessage: string = this.isPathLegal(newPath, "Temp");
            return errorMessage;
        });
    }

    attached() {
    }

    deactivate() {
        // If we were closed via X button or other dialog dismissal, reject the deletion task since
        // we never started it.
        if (!this.creationTaskStarted) {
            this.creationTask.reject();
        }
    }

    isBundleActive(name: string): boolean {
        var licenseStatus: licenseStatusDto = this.licenseStatus();

        if (licenseStatus == null || licenseStatus.IsCommercial == false) {
            return true;
        }
        else {
            var value = licenseStatus.Attributes[name];
            return value === "true";
        }
    }

    nextOrCreate() {
        // Next needs to configure bundle settings, if we've selected some bundles.
        // We haven't yet implemented bundle configuration, so for now we're just 
        // creating the database.

        this.creationTaskStarted = true;
        dialog.close(this.parent);
        this.creationTask.resolve(this.databaseName(), this.getActiveBundles(), this.databasePath(), this.databaseLogsPath(), this.databaseIndexesPath(), this.databaseTempPath(), this.storageEngine(),
            this.isIncrementalBackupChecked(), this.alertTimeout(), this.alertRecurringTimeout());
    }

    private isDatabaseNameExists(databaseName: string, databases: database[]): boolean {
        databaseName = databaseName.toLowerCase();
        for (var i = 0; i < databases.length; i++) {
            if (databaseName == databases[i].name.toLowerCase()) {
                return true;
            }
        }
        return false;
    }

    private checkName(name: string): string {
        var rg1 = /^[^\\/\*:\?"<>\|]+$/; // forbidden characters \ / * : ? " < > |
        var rg2 = /^\./; // cannot start with dot (.)
        var rg3 = /^(nul|prn|con|lpt[0-9]|com[0-9])(\.|$)/i; // forbidden file names

        var message = "";
        if (!$.trim(name)) {
            message = "Please fill out the database name field!";
        }
        else if (name.length > this.maxNameLength) {
            message = "The database name length can't exceed " + this.maxNameLength + " characters!";
        }
        else if (!rg1.test(name)) {
            message = "The database name can't contain any of the following characters: \ / * : ?" + ' " ' + "< > |";
        }
        else if (rg2.test(name)) {
            message = "The database name can't start with a dot!";
        }
        else if (rg3.test(name)) {
            message = "The name '" + name + "' is forbidden for use!";
        }
        else if (name[name.length-1]==".") {
            message = "The database name can't end with a dot!";
        }
        else if (name.toLocaleLowerCase() == "system") {
            message = "This name is reserved for the actual system database!";
        }
        return message;
    }

    private isPathLegal(name: string, pathName: string): string {
        var rg1 = /^[^*\?"<>\|]+$/; // forbidden characters \ * : ? " < > |
        var rg2 = /^(nul|prn|con|lpt[0-9]|com[0-9])(\.|$)/i; // forbidden file names
        var errorMessage = "";

        if (!$.trim(name) == false) { // if name isn't empty or not consist of only whitepaces
            if (name.length > 248) {
                errorMessage = "The path name for the '" + pathName + "' can't exceed " + 248 + " characters!";
            } else if (!rg1.test(name)) {
                errorMessage = "The " + pathName + " can't contain any of the following characters: * : ?" + ' " ' + "< > |";
            } else if (rg2.test(name)) {
                errorMessage = "The name '" + name + "' is forbidden for use!";
            }
        }
        return errorMessage;
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

        return activeBundles;
    }
}

export = createDatabase;

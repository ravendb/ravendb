import app = require("durandal/app");
import document = require("models/document");
import dialog = require("plugins/dialog");
import createDatabaseCommand = require("commands/createDatabaseCommand");
import collection = require("models/collection");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import database = require("models/database");
import commandBase = require("commands/commandBase");

class createDatabase extends dialogViewModelBase {

    public creationTask = $.Deferred();
    creationTaskStarted = false;

    databaseName = ko.observable('');
    databasePath = ko.observable('');
    databaseLogs = ko.observable('');
    databaseIndexes = ko.observable('');
    databaseNameFocus = ko.observable(true);
    isCompressionBundleEnabled = ko.observable(false);
    isEncryptionBundleEnabled = ko.observable(false);
    isExpirationBundleEnabled = ko.observable(false);
    isQuotasBundleEnabled = ko.observable(false);
    isReplicationBundleEnabled = ko.observable(false);
    isSqlReplicationBundleEnabled = ko.observable(false);
    isVersioningBundleEnabled = ko.observable(false);
    isPeriodicBackupBundleEnabled = ko.observable(true); // Old Raven Studio has this enabled by default
    isScriptedIndexBundleEnabled = ko.observable(false);

    private databases = ko.observableArray<database>();
    private maxNameLength = 260 - 30;
    private newCommandBase = new commandBase();
    
    constructor(databases) {
        super();
        this.databases = databases;
    }

    cancel() {
        dialog.close(this);
    }

    attached() {
        super.attached();
        this.databaseNameFocus(true);
    }

    deactivate() {
        // If we were closed via X button or other dialog dismissal, reject the deletion task since
        // we never started it.
        if (!this.creationTaskStarted) {
            this.creationTask.reject();
        }
    }

    nextOrCreate() {
        // Next needs to configure bundle settings, if we've selected some bundles.
        // We haven't yet implemented bundle configuration, so for now we're just 
        // creating the database.
        var databaseName = this.databaseName();
        var databasePath = this.databasePath();
        var databaseLogs = this.databaseLogs();
        var databaseIndexes = this.databaseIndexes();

        if (this.isDatabaseNameLegal(databaseName) && this.arePathsLegal(databasePath, databaseLogs, databaseIndexes)) {
            this.creationTaskStarted = true;
            this.creationTask.resolve(databaseName, this.getActiveBundles(), databasePath, databaseLogs, databaseIndexes);
            dialog.close(this);
        }
    }

    private arePathsLegal(databasePath: string, databaseLogs: string, databaseIndexes: string) {
        if (this.isPathLegal(databasePath, "Path") && this.isPathLegal(databaseLogs, "Logs") && this.isPathLegal(databaseIndexes, "Indexes")) {
            return true;
        }
        return false;
    }

    private isPathLegal(name: string, pathName: string) {
        var rg1 = /^[^\\*:\?"<>\|]+$/; // forbidden characters \ * : ? " < > |
        var rg2 = /^(nul|prn|con|lpt[0-9]|com[0-9])(\.|$)/i; // forbidden file names
        var errorMessage = null;

        if (!$.trim(name) == false) { // if name isn't empty or not consist of only whitepaces
            if (name.length > this.maxNameLength) {
                errorMessage = "The path name for the '" + pathName + "' can't exceed " + this.maxNameLength + " characters!";
            } else if (!rg1.test(name)) {
                errorMessage = "The " + pathName + " can't contain any of the following characters: * : ?" + ' " ' + "< > |";
            } else if (rg2.test(name)) {
                errorMessage = "The name '" + name + "' is forbidden for use!";
            }
        }
        if (errorMessage != null) {
            this.newCommandBase.reportError(errorMessage);
            return false;
        }
        return true;
    }

    private isDatabaseNameLegal(databaseName: string): boolean {
        var errorMessage = null;

        if (databaseName == null) {
            errorMessage = "Please fill out the Database Name field";
        }
        else if (this.isDatabaseNameExists(databaseName, this.databases()) === true) {
            errorMessage = "Database Name Already Exists!";
        }
        else if ((errorMessage = this.CheckName(databaseName)) != null) { }

        if (errorMessage != null) {
            this.newCommandBase.reportError(errorMessage);
            this.databaseNameFocus(true);
            return false;
        }
        return true;
    }

    private CheckName(name: string): string {
        var rg1 = /^[^\\/\*:\?"<>\|]+$/; // forbidden characters \ / * : ? " < > |
        var rg2 = /^\./; // cannot start with dot (.)
        var rg3 = /^(nul|prn|con|lpt[0-9]|com[0-9])(\.|$)/i; // forbidden file names

        var message = null;
        if (!$.trim(name)) {
            message = "An empty databse name is forbidden for use!";
        }
        else if (name.length > this.maxNameLength) {
            message = "The database length can't exceed " + this.maxNameLength + " characters!";
        }
        else if (!rg1.test(name)) {
            message = "The database name can't contain any of the following characters: \ / * : ?" + ' " ' +"< > |";
        }
        else  if (rg2.test(name)) {
            message = "The database name can't start with a dot!";
        }
        else if (rg3.test(name)) {
            message = "The name '" + name + "' is forbidden for use!";
        }
        return message;
    }

    private isDatabaseNameExists(databaseName: string, databases: database[]): boolean {
        for (var i = 0; i < databases.length; i++) {
            if (databaseName == databases[i].name) {
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
        if (this.isQuotasBundleEnabled() === false)
            app.showMessage("Quotas Bundle configuration window is not implemented yet.", "Not implemented");
        this.isQuotasBundleEnabled.toggle();
    }

    toggleReplicationBundle() {
        this.isReplicationBundleEnabled.toggle();
    }

    toggleSqlReplicationBundle() {
        this.isSqlReplicationBundleEnabled.toggle();
    }

    toggleVersioningBundle() {
        if (this.isVersioningBundleEnabled() === false)
            app.showMessage("Versioning Bundle configuration window is not implemented yet.", "Not implemented");
        this.isVersioningBundleEnabled.toggle();
    }

    togglePeriodicBackupBundle() {
        this.isPeriodicBackupBundleEnabled.toggle();
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

        if (this.isPeriodicBackupBundleEnabled()) {
            activeBundles.push("PeriodicBackups");
        }

        if (this.isScriptedIndexBundleEnabled()) {
            activeBundles.push("ScriptedIndexResults");
        }
        return activeBundles;
    }
}

export = createDatabase;
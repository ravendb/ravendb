import app = require("durandal/app");
import document = require("models/document");
import dialog = require("plugins/dialog");
import createDatabaseCommand = require("commands/createDatabaseCommand");
import collection = require("models/collection");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import database = require("models/database");
import getLicenseStatusCommand = require("commands/getLicenseStatusCommand");

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
    isPeriodicExportBundleEnabled = ko.observable(true); // Old Raven Studio has this enabled by default
    isScriptedIndexBundleEnabled = ko.observable(false);

    licenseStatus = ko.observable<licenseStatusDto>(null);
    
    isCompressionBundleActive: KnockoutComputed<boolean>;
    isEncryptionBundleActive: KnockoutComputed<boolean>;
    isExpirationBundleActive: KnockoutComputed<boolean>;
    isQuotasBundleActive: KnockoutComputed<boolean>;
    isReplicationBundleActive: KnockoutComputed<boolean>;
    isSqlReplicationBundleActive: KnockoutComputed<boolean>;
    isVersioningBundleActive: KnockoutComputed<boolean>;
    isPeriodicExportBundleActive: KnockoutComputed<boolean>;
    isScriptedIndexBundleActive: KnockoutComputed<boolean>;


    private databases = ko.observableArray<database>();
    private maxNameLength = 200;

    constructor(databases) {
        super();
        this.databases = databases;
        this.isCompressionBundleActive = ko.computed<boolean>(() => this.licenseStatus() === null || this.licenseStatus().IsCommercial == false || this.licenseStatus().Attributes.compression === "true");
        this.isEncryptionBundleActive = ko.computed<boolean>(() => this.licenseStatus() === null || this.licenseStatus().IsCommercial == false|| this.licenseStatus().Attributes.encryption === "true");
        this.isExpirationBundleActive = ko.computed<boolean>(() => this.licenseStatus() === null || this.licenseStatus().IsCommercial == false|| this.licenseStatus().Attributes.documentExpiration === "true");
        this.isQuotasBundleActive = ko.computed<boolean>(() => this.licenseStatus() === null || this.licenseStatus().IsCommercial == false|| this.licenseStatus().Attributes.quotas === "true");
        this.isReplicationBundleActive = ko.computed<boolean>(() => this.licenseStatus() === null || this.licenseStatus().IsCommercial == false|| this.licenseStatus().Attributes.replication === "true");
        this.isVersioningBundleActive = ko.computed<boolean>(() => this.licenseStatus() === null || this.licenseStatus().IsCommercial == false|| this.licenseStatus().Attributes.versioning === "true");
        this.isPeriodicExportBundleActive = ko.computed<boolean>(() => this.licenseStatus() === null || this.licenseStatus().IsCommercial == false|| this.licenseStatus().Attributes.periodicBackup === "true");

        this.isScriptedIndexBundleActive = ko.computed<boolean>(() => true);
        this.isSqlReplicationBundleActive = ko.computed<boolean>(() => true);
    }

    attached() {
        super.attached();
        this.databaseNameFocus(true);
        
        var inputElement: any = $("#databaseName")[0];
        this.databaseName.subscribe((newDatabaseName) => {
            var errorMessage: string = '';
            if (this.isDatabaseNameExists(newDatabaseName, this.databases()) === true) {
                errorMessage = "Database Name Already Exists!";
            }
            else if ((errorMessage = this.CheckName(newDatabaseName)) != '') { }
            inputElement.setCustomValidity(errorMessage);
        });
    
        inputElement.setCustomValidity("An empty database name is forbidden for use!");
        

        this.subscribeToPath("#databasePath", this.databasePath, "Path");
        this.subscribeToPath("#databaseLogs", this.databaseLogs, "Logs");
        this.subscribeToPath("#databaseIndexes", this.databaseIndexes, "Indexes");

        this.fetchLicenseStatus();
    }

    deactivate() {
        // If we were closed via X button or other dialog dismissal, reject the deletion task since
        // we never started it.
        if (!this.creationTaskStarted) {
            this.creationTask.reject();
        }
    }
     cancel() {
        dialog.close(this);
    }

    nextOrCreate() {
        // Next needs to configure bundle settings, if we've selected some bundles.
        // We haven't yet implemented bundle configuration, so for now we're just 
        // creating the database.

        this.creationTaskStarted = true;
        dialog.close(this);
        this.creationTask.resolve(this.databaseName(), this.getActiveBundles(), this.databasePath(), this.databaseLogs(), this.databaseIndexes());
        
    }

    private isDatabaseNameExists(databaseName: string, databases: database[]): boolean {
        for (var i = 0; i < databases.length; i++) {
            if (databaseName == databases[i].name) {
                return true;
            }
        }
        return false;
    }

    private CheckName(name: string): string {
        var rg1 = /^[^\\/\*:\?"<>\|]+$/; // forbidden characters \ / * : ? " < > |
        var rg2 = /^\./; // cannot start with dot (.)
        var rg3 = /^(nul|prn|con|lpt[0-9]|com[0-9])(\.|$)/i; // forbidden file names

        var message = '';
        if (!$.trim(name)) {
            message = "An empty database name is forbidden for use!";
        }
        else if (name.length > this.maxNameLength) {
            message = "The database length can't exceed " + this.maxNameLength + " characters!";
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
            message = "The database name can't end with a dot !";
        }
        return message;
    }

    private subscribeToPath(tag, element, pathName) {
        var inputElement: any = $(tag)[0];
        element.subscribe((path) => {
            var errorMessage: string = this.isPathLegal(path, pathName);
            inputElement.setCustomValidity(errorMessage);
        });
    }

    private isPathLegal(name: string, pathName: string): string {
        var rg1 = /^[^*\?"<>\|]+$/; // forbidden characters \ * : ? " < > |
        var rg2 = /^(nul|prn|con|lpt[0-9]|com[0-9])(\.|$)/i; // forbidden file names
        var errorMessage = null;

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
        if (this.isCompressionBundleActive()) {
            this.isCompressionBundleEnabled.toggle();
        }
    }

    toggleEncryptionBundle() {
        if (this.isEncryptionBundleActive()) {
            this.isEncryptionBundleEnabled.toggle();
        }
    }

    toggleExpirationBundle() {
        if (this.isExpirationBundleActive()) {
            this.isExpirationBundleEnabled.toggle();
        }
    }

    toggleQuotasBundle() {
        if (this.isQuotasBundleActive()) {
            this.isQuotasBundleEnabled.toggle();
        }
    }

    toggleReplicationBundle() {
        if (this.isReplicationBundleActive()) {
            this.isReplicationBundleEnabled.toggle();
        }
    }

    toggleSqlReplicationBundle() {
        if (this.isSqlReplicationBundleActive()) {
            this.isSqlReplicationBundleEnabled.toggle();
        }
    }

    toggleVersioningBundle() {
        if (this.isVersioningBundleActive()) {
            this.isVersioningBundleEnabled.toggle();
        }
    }

    togglePeriodicExportBundle() {
        if (this.isPeriodicExportBundleActive()) {
            this.isPeriodicExportBundleEnabled.toggle();
        }
    }

    toggleScriptedIndexBundle() {
        if (this.isScriptedIndexBundleActive()) {
            this.isScriptedIndexBundleEnabled.toggle();
        }
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

    fetchLicenseStatus() {
        new getLicenseStatusCommand()
            .execute()
            .done((result: licenseStatusDto) => {
                if (result.Attributes.periodicBackup !== "true") {
                    this.isPeriodicExportBundleEnabled(false);
                }
                this.licenseStatus(result);
            });
    }

    canUseBundle(name: string): boolean {
        if (this.licenseStatus() === null) {
            return true;
        }
        else {
            var value = this.licenseStatus().Attributes[name];
        }
    }
}

export = createDatabase;

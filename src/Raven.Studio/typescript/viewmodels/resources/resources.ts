import app = require("durandal/app");
import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import shell = require("viewmodels/shell");
import accessHelper = require("viewmodels/shell/accessHelper");

import resource = require("models/resources/resource");
import database = require("models/resources/database");

import deleteResourceConfirm = require("viewmodels/resources/deleteResourceConfirm");
import disableResourceToggleConfirm = require("viewmodels/resources/disableResourceToggleConfirm");
import disableIndexingCommand = require("commands/database/index/disableIndexingCommand");
import toggleRejectDatabaseClients = require("commands/maintenance/toggleRejectDatabaseClients");
import createResource = require("viewmodels/resources/createResource");
import createEncryption = require("viewmodels/resources/createEncryption");
import createDatabaseCommand = require("commands/resources/createDatabaseCommand");
import createEncryptionConfirmation = require("viewmodels/resources/createEncryptionConfirmation");
import databaseSettingsDialog = require("viewmodels/resources/databaseSettingsDialog");
import createDefaultDbSettingsCommand = require("commands/resources/createDefaultSettingsCommand");

import filesystemSettingsDialog = require("viewmodels/resources/filesystemSettingsDialog");

import fileSystem = require("models/filesystem/filesystem");
import createDefaultFsSettingsCommand = require("commands/filesystem/createDefaultSettingsCommand");
import createFilesystemCommand = require("commands/filesystem/createFilesystemCommand");
import counterStorage = require("models/counter/counterStorage");
import createCounterStorageCommand = require("commands/resources/createCounterStorageCommand");
import timeSeries = require("models/timeSeries/timeSeries");
import createTimeSeriesCommand = require("commands/resources/createTimeSeriesCommand");
import license = require("models/auth/license");

class resources extends viewModelBase {
    resources: KnockoutComputed<resource[]>;

    databases = ko.observableArray<database>();
    fileSystems = ko.observableArray<fileSystem>();
    counterStorages = ko.observableArray<counterStorage>();
    timeSeries = ko.observableArray<timeSeries>();
    searchText = ko.observable("");
    selectedResource = ko.observable<resource>();
    fileSystemsStatus = ko.observable<string>("loading");
    resourcesSelection: KnockoutComputed<checkbox>;
    allCheckedResourcesDisabled: KnockoutComputed<boolean>;
    isCheckboxVisible: KnockoutComputed<boolean>;
    systemDb: database;
    optionsClicked = ko.observable<boolean>(false);
    appUrls: computedAppUrls;
    isGlobalAdmin = accessHelper.isGlobalAdmin;
    clusterMode = ko.computed(() => shell.clusterMode());
    developerLicense = ko.computed(() => !license.licenseStatus() || !license.licenseStatus().IsCommercial);
    showCreateCluster = ko.computed(() => !shell.clusterMode());
    canCreateCluster = ko.computed(() => license.licenseStatus() && (!license.licenseStatus().IsCommercial || license.licenseStatus().Attributes.clustering === "true"));
    canNavigateToAdminSettings: KnockoutComputed<boolean>;

    databaseType = database.type;
    fileSystemType = fileSystem.type;
    counterStorageType = counterStorage.type;
    timeSeriesType = timeSeries.type;
    visibleResource = ko.observable("");
    visibleOptions: { value:string, name: string }[];
    databasesSummary: KnockoutComputed<string>;
    fileSystemsSummary: KnockoutComputed<string>;
    isCommaNeeded: KnockoutComputed<boolean>;

    constructor() {
        super();

        this.visibleOptions = [
            { value: "", name: "Show all" },
            { value: database.type, name: "Show databases" },
            { value: fileSystem.type, name: "Show file systems" },
            { value: counterStorage.type, name: "Show counter storages" },
            { value: timeSeries.type, name: "Show time series" }
        ];

        this.canNavigateToAdminSettings = ko.computed(() =>
            accessHelper.isGlobalAdmin() || accessHelper.canReadWriteSettings() || accessHelper.canReadSettings());

        this.databases = shell.databases;
        this.fileSystems = shell.fileSystems;
        this.counterStorages = shell.counterStorages;
        this.timeSeries = shell.timeSeries;
        this.resources = shell.resources;

        // uncheck all during page load
        this.resources().forEach(resource => resource.isChecked(false));
        
        this.appUrls = appUrl.forCurrentDatabase(); 
        this.searchText.extend({ throttle: 200 }).subscribe(() => this.filterResources());

        var currentDatabase = this.activeDatabase();
        if (!!currentDatabase) {
            this.selectResource(currentDatabase, false);
        }

        var currentFileSystem = this.activeFilesystem();
        if (!!currentFileSystem) {
            this.selectResource(currentFileSystem, false);
        }

        var currentCounterStorage = this.activeCounterStorage();
        if (!!currentCounterStorage) {
            this.selectResource(currentCounterStorage, false);
        }

        var currentTimeSeries = this.activeTimeSeries();
        if (!!currentTimeSeries) {
            this.selectResource(currentTimeSeries, false);
        }

        var updatedUrl = appUrl.forResources();
        this.updateUrl(updatedUrl);

        this.resourcesSelection = ko.computed(() => {
            var resources = this.resources();
            if (resources.length === 0) {
                return checkbox.UnChecked;
            }

            var allSelected = true;
            var anySelected = false;

            for (var i = 0; i < resources.length; i++) {
                var rs: resource = resources[i];
                if (rs.isDatabase() && (<any>rs).isSystem) {
                    continue;
                }

                if (rs.isChecked()) {
                    anySelected = true;
                } else {
                    allSelected = false;
                }
            }

            if (allSelected) {
                return checkbox.Checked;
            } else if (anySelected) {
                return checkbox.SomeChecked;
            }
            return checkbox.UnChecked;
        });

        this.allCheckedResourcesDisabled = ko.computed(() => {
            var disabledStatus: boolean = null;
            for (var i = 0; i < this.resources().length; i++) {
                var rs: resource = this.resources()[i];
                if (rs.isChecked()) {
                    if (disabledStatus == null) {
                        disabledStatus = rs.disabled();
                    } else if (disabledStatus !== rs.disabled()) {
                        return null;
                    }
                }
            }

            return disabledStatus;
        });

        this.isCheckboxVisible = ko.computed(() => {
            if (!this.isGlobalAdmin())
                return false;

            var resources = this.resources();
            for (var i = 0; i < resources.length; i++) {
                var rs: resource = resources[i];
                if (rs.isVisible())
                    return true;
            }
            return false;
        });

        this.databasesSummary = ko.computed(() => this.getResourcesSummary(this.databases(), "database"));
        this.fileSystemsSummary = ko.computed(() => this.getResourcesSummary(this.fileSystems(), "file system"));
        this.isCommaNeeded = ko.computed(() => 
            this.databases().filter(x => x.isVisible()).length > 0 &&
            this.fileSystems().filter(x => x.isVisible()).length > 0);

        this.visibleResource.subscribe(() => this.filterResources());
        this.filterResources();
    }

    private getResourcesSummary(resourcesCollection: Array<resource>, type: string) {
        var summary = "";

        var resources = resourcesCollection.filter(x => x.isVisible());
        if (resources.length > 0) {
            summary += resources.length + " "  + type;
            if (resources.length > 1) {
                summary += "s";
            }

            var disabled = resources.filter(x => x.disabled()).length;
            if (disabled > 0) {
                summary += " (" + disabled + " disabled)";
            }
        }

        return summary;
    }

    // Override canActivate: we can always load this page, regardless of any system db prompt.
    canActivate(args: any): any {
        return true;
    }

    attached() {
        super.attached();
        this.updateHelpLink("Z8DC3Q");
        ko.postbox.publish("SetRawJSONUrl", appUrl.forDatabasesRawData());
        this.resourcesLoaded();
    }

    private resourcesLoaded() {
        /*

        Show it only when cluster bundle is not present
        // If we have no databases (except system db), show the "create a new database" screen.
        if (this.resources().length === 1 && this.isGlobalAdmin()) {
            this.newResource();
        } */
    }

    filterResources() {
        var filter = this.searchText();
        var filterLower = filter.toLowerCase();
        this.databases().forEach((db: database) => {
            var typeMatch = !this.visibleResource() || this.visibleResource() === database.type;
            var isMatch = (!filter || (db.name.toLowerCase().indexOf(filterLower) >= 0)) && db.name !== "<system>" && typeMatch;
            db.isVisible(isMatch);
        });
        this.databases().map((db: database) => db.isChecked(!db.isVisible() ? false : db.isChecked()));

        this.fileSystems().forEach(fs => {
            var typeMatch = !this.visibleResource() || this.visibleResource() === fileSystem.type;
            var isMatch = (!filter || (fs.name.toLowerCase().indexOf(filterLower) >= 0)) && typeMatch;
            fs.isVisible(isMatch);
        });
        this.fileSystems().map((fs: fileSystem) => fs.isChecked(!fs.isVisible() ? false : fs.isChecked()));

        this.counterStorages().forEach(cs => {
            var typeMatch = !this.visibleResource() || this.visibleResource() === counterStorage.type;
            var isMatch = (!filter || (cs.name.toLowerCase().indexOf(filterLower) >= 0)) && typeMatch;
            cs.isVisible(isMatch);
        });
        this.counterStorages().map((cs: counterStorage) => cs.isChecked(!cs.isVisible() ? false : cs.isChecked()));

        this.timeSeries().forEach(ts => {
            var typeMatch = !this.visibleResource() || this.visibleResource() === timeSeries.type;
            var isMatch = (!filter || (ts.name.toLowerCase().indexOf(filterLower) >= 0)) && typeMatch;
            ts.isVisible(isMatch);
        });
        this.timeSeries().map((ts: timeSeries) => ts.isChecked(!ts.isVisible() ? false : ts.isChecked()));
    }

    getDocumentsUrl(db: database) {
        return appUrl.forDocuments(null, db);
    }

    getFileSystemFilesUrl(fs: fileSystem) {
        return appUrl.forFilesystemFiles(fs);
    }

    getCounterStorageCountersUrl(cs: counterStorage) {
        return appUrl.forCounterStorageCounters(null, cs);
    }

    getTimeSeriesUrl(ts: timeSeries) {
        return appUrl.forTimeSeriesType(null, ts);
    }

    selectResource(rs: resource, activateResource: boolean = true) {
        if (this.optionsClicked() === false) {
            if (activateResource) {
                rs.activate();
            }
            this.selectedResource(rs);
        }

        this.optionsClicked(false);
    }

    deleteSelectedResources(resources: Array<resource>) {
        if (resources.length > 0) {
            var confirmDeleteViewModel = new deleteResourceConfirm(resources);

            confirmDeleteViewModel.deleteTask.done((deletedResources: Array<resource>) => {
                if (resources.length === 1) {
                    this.onResourceDeleted(resources[0]);
                } else {
                    deletedResources.forEach(rs => this.onResourceDeleted(rs));
                }
            });

            app.showDialog(confirmDeleteViewModel);
        }
    }

    private onResourceDeleted(resourceToDelete: resource) {
        var resourcesArray = this.getResources(resourceToDelete.type);

        var resourceInArray = resourcesArray.first((db: resource) => db.name === resourceToDelete.name);
        if (!!resourceInArray) {
            resourcesArray.remove(resourceInArray);
        }

        if ((this.resources().length > 0) && (this.resources().contains(this.selectedResource()) === false)) {
            ko.postbox.publish("SelectNone");
        }
    }

    private getResources(resourceType: TenantType): KnockoutObservableArray<resource> {
        switch (resourceType) {
            case TenantType.Database:
                return this.databases;
            case TenantType.FileSystem:
                return this.fileSystems;
            case TenantType.CounterStorage:
                return this.counterStorages;
            case TenantType.TimeSeries:
                return this.timeSeries;
            default:
                throw "Unknown type";
        }
    }

    deleteCheckedResources() {
        var checkedResources: resource[] = this.resources().filter((rs: resource) => rs.isChecked());
        this.deleteSelectedResources(checkedResources);
    }

    toggleSelectAll() {
        var check = true;

        if (this.resourcesSelection() !== checkbox.UnChecked) {
            check = false;
        }

        for (var i = 0; i < this.resources().length; i++) {
            var rs: resource = this.resources()[i];
            if (rs.isDatabase()) {
                rs.isChecked(false);
                continue;
            }
            if (rs.isVisible()) {   
                rs.isChecked(check);
            }
        }
    }

    toggleCheckedResources() {
        var checkedResources: resource[] = this.resources().filter((rs: resource) => rs.isChecked());
        this.toggleSelectedResources(checkedResources);
    }

    toggleSelectedResources(resources: resource[]) {
        if (resources.length > 0) {
            var action = !resources[0].disabled();

            var disableDatabaseToggleViewModel = new disableResourceToggleConfirm(resources);

            disableDatabaseToggleViewModel.disableToggleTask
                .done((toggledResources: resource[]) => {
                    if (resources.length === 1) {
                        this.onResourceDisabledToggle(resources[0], action);
                    } else {
                        toggledResources.forEach(rs => {
                            this.onResourceDisabledToggle(rs, action);
                        });
                    }
                });

            app.showDialog(disableDatabaseToggleViewModel);
        }
    }

    private onResourceDisabledToggle(rs: resource, action: boolean) {
        if (!!rs) {
            rs.disabled(action);
            rs.isChecked(false);

            if (rs.isSelected() && rs.disabled() === false) {
                rs.activate();  
            }
        }
    }

    disableDatabaseIndexing(db: database) {
        var action = !db.indexingDisabled();
        var actionText = db.indexingDisabled() ? "Enable" : "Disable"; 
        var message = this.confirmationMessage(actionText + " indexing?", "Are you sure?");
        
        message.done(() => {
            var task = new disableIndexingCommand(db.name, action).execute();
            task.done(() => db.indexingDisabled(action));
        });
    }

    toggleRejectDatabaseClients(db: database) {
        var action = !db.rejectClientsMode();
        var actionText = action ? "reject clients mode" : "accept clients mode";
        var message = this.confirmationMessage("Switch to " + actionText, "Are you sure?");
        message.done(() => {
            var task = new toggleRejectDatabaseClients(db.name, action).execute();
            task.done(() => db.rejectClientsMode(action));
        });
    }
    
    navigateToCreateCluster() {
        this.navigate(this.appUrls.adminSettingsCluster());
        shell.disconnectFromResourceChangesApi();
    }

    newResource() {
        var createResourceViewModel = new createResource();
        createResourceViewModel.createDatabasePart
            .creationTask
            .done((databaseName: string, bundles: string[], databasePath: string, databaseLogs: string, databaseIndexes: string, tempPath: string, storageEngine: string, incrementalBackup: boolean
                , alertTimeout: string, alertRecurringTimeout: string, clusterWide: boolean) => {
                var settings: dictionary<string> = {
                    "Raven/ActiveBundles": bundles.join(";")
                };
                if (storageEngine) {
                    settings["Raven/StorageTypeName"] = storageEngine;
                }
                if (!clusterWide) {
                    settings["Raven-Non-Cluster-Database"] = "true";
                }
                if (incrementalBackup) {
                    if (storageEngine === "esent") {
                        settings["Raven/Esent/CircularLog"] = "false";
                    } else {
                        settings["Raven/Voron/AllowIncrementalBackups"] = "true";
                    }
                }
                if (!this.isEmptyStringOrWhitespace(tempPath)) {
                    settings["Raven/Voron/TempPath"] = tempPath;
                }
                if (alertTimeout !== "") {
                    settings["Raven/IncrementalBackup/AlertTimeoutHours"] = alertTimeout;
                }
                if (alertRecurringTimeout !== "") {
                    settings["Raven/IncrementalBackup/RecurringAlertTimeoutDays"] = alertRecurringTimeout;
                }
                settings["Raven/DataDir"] = (!this.isEmptyStringOrWhitespace(databasePath)) ? databasePath : "~/" + databaseName;
                if (!this.isEmptyStringOrWhitespace(databaseLogs)) {
                    settings["Raven/Esent/LogsPath"] = databaseLogs;
                }
                if (!this.isEmptyStringOrWhitespace(databaseIndexes)) {
                    settings["Raven/IndexStoragePath"] = databaseIndexes;
                }

                this.showDbCreationAdvancedStepsIfNecessary(databaseName, bundles, settings, clusterWide);
            });

        createResourceViewModel.createFileSystemPart
            .creationTask
            .done((fileSystemName: string, bundles: string[], fileSystemPath: string, filesystemLogs: string, tempPath: string, storageEngine: string) => {
                var settings: dictionary<string> = {
                    "Raven/ActiveBundles": bundles.join(";")
                }

                settings["Raven/FileSystem/DataDir"] = (!this.isEmptyStringOrWhitespace(fileSystemPath)) ? fileSystemPath : "~\\FileSystems\\" + fileSystemName;
                if (storageEngine) {
                    settings["Raven/FileSystem/Storage"] = storageEngine;
                }
                if (!this.isEmptyStringOrWhitespace(filesystemLogs)) {
                    settings["Raven/TransactionJournalsPath"] = filesystemLogs;
                }
                if (!this.isEmptyStringOrWhitespace(tempPath)) {
                    settings["Raven/Voron/TempPath"] = tempPath;
                }
                this.showFsCreationAdvancedStepsIfNecessary(fileSystemName, bundles, settings);
            });

        createResourceViewModel.createCounterStoragePart
            .creationTask
            .done((counterStorageName: string, bundles: string[], counterStoragePath: string, tempPath: string) => {
                var settings: dictionary<string> = {
                    "Raven/ActiveBundles": bundles.join(";")
                }
                settings["Raven/Counters/DataDir"] = (!this.isEmptyStringOrWhitespace(counterStoragePath)) ? counterStoragePath : "~\\Counters\\" + counterStorageName;
                if (!this.isEmptyStringOrWhitespace(tempPath)) {
                    settings["Raven/Voron/TempPath"] = tempPath;
                }

                this.showCsCreationAdvancedStepsIfNecessary(counterStorageName, bundles, settings);
            });

        createResourceViewModel.createTimeSeriesPart
            .creationTask
            .done((timeSeriesName: string, bundles: string[], timeSeriesPath: string, tempPath: string) => {
                var settings: dictionary<string> = {
                    "Raven/ActiveBundles": bundles.join(";")
                }
                settings["Raven/TimeSeries/DataDir"] = (!this.isEmptyStringOrWhitespace(timeSeriesPath)) ? timeSeriesPath : "~\\TimeSeries\\" + timeSeriesName;
                if (!this.isEmptyStringOrWhitespace(tempPath)) {
                    settings["Raven/Voron/TempPath"] = tempPath;
                }

                this.showTsCreationAdvancedStepsIfNecessary(timeSeriesName, bundles, settings);
            });

        app.showDialog(createResourceViewModel);
    }

    private showDbCreationAdvancedStepsIfNecessary(databaseName: string, bundles: string[], settings: dictionary<string>, clusterWide: boolean) {
        var securedSettings = {};
        var savedKey: string;

        var encryptionDeferred = $.Deferred();

        if (bundles.contains("Encryption")) {
            var createEncryptionViewModel = new createEncryption();
            createEncryptionViewModel
                .creationEncryption
                .done((key: string, encryptionAlgorithm: string, encryptionBits: string, isEncryptedIndexes: string) => {
                    savedKey = key;
                    securedSettings = {
                        'Raven/Encryption/Key': key,
                        'Raven/Encryption/Algorithm': this.getEncryptionAlgorithmFullName(encryptionAlgorithm),
                        'Raven/Encryption/KeyBitsPreference': encryptionBits,
                        'Raven/Encryption/EncryptIndexes': isEncryptedIndexes
                    };
                    encryptionDeferred.resolve(securedSettings);
                });
            app.showDialog(createEncryptionViewModel);
        } else {
            encryptionDeferred.resolve();
        }

        encryptionDeferred.done(() => {
            new createDatabaseCommand(databaseName, settings, securedSettings)
                .execute()
                .done(() => {
                    var newDatabase = this.addNewDatabase(databaseName, bundles, clusterWide);
                    this.selectResource(newDatabase);

                    var encryptionConfirmationDialogPromise = $.Deferred();
                    if (!jQuery.isEmptyObject(securedSettings)) {
                        var createEncryptionConfirmationViewModel = new createEncryptionConfirmation(savedKey);
                        createEncryptionConfirmationViewModel.dialogPromise.done(() => encryptionConfirmationDialogPromise.resolve());
                        createEncryptionConfirmationViewModel.dialogPromise.fail(() => encryptionConfirmationDialogPromise.reject());
                        app.showDialog(createEncryptionConfirmationViewModel);
                    } else {
                        encryptionConfirmationDialogPromise.resolve();
                    }

                    this.createDefaultDatabaseSettings(newDatabase, bundles).always(() => {
                        if (bundles.contains("Quotas") || bundles.contains("Versioning") || bundles.contains("SqlReplication")) {
                            encryptionConfirmationDialogPromise.always(() => {
                                // schedule dialog using setTimeout to avoid issue with dialog width
                                // (it isn't recalculated when dialog is already opened)
                                setTimeout(() => {
                                        var settingsDialog = new databaseSettingsDialog(bundles);
                                        app.showDialog(settingsDialog);
                                    }, 1);
                            });
                        }
                    });
                });
        });
    }

    private addNewDatabase(databaseName: string, bundles: string[], clusterWide: boolean): database {
        var foundDatabase = this.databases.first((db: database) => db.name == databaseName);

        if (!foundDatabase) {
            var newDatabase = new database(databaseName, true, false, bundles, undefined, undefined, clusterWide);
            this.databases.unshift(newDatabase);
            this.filterResources();
            return newDatabase;
        }
        return foundDatabase;
    }

    private createDefaultDatabaseSettings(db: database, bundles: Array<string>): JQueryPromise<any> {
        var deferred = $.Deferred();
        new createDefaultDbSettingsCommand(db, bundles).execute()
            .always(() => deferred.resolve());
        return deferred;
    }

    private createDefaultFilesystemSettings(fs: fileSystem, bundles: Array<string>): JQueryPromise<any> {
        var deferred = $.Deferred();
        new createDefaultFsSettingsCommand(fs, bundles).execute()
            .always(() => deferred.resolve());
        return deferred;
    }

    private isEmptyStringOrWhitespace(str: string) {
        return !$.trim(str);
    }

    private getEncryptionAlgorithmFullName(encrytion: string) {
        var fullEncryptionName: string = null;
        switch (encrytion) {
            case "DES":
                fullEncryptionName = "System.Security.Cryptography.DESCryptoServiceProvider, mscorlib";
                break;
            case "RC2":
                fullEncryptionName = "System.Security.Cryptography.RC2CryptoServiceProvider, mscorlib";
                break;
            case "Rijndael":
                fullEncryptionName = "System.Security.Cryptography.RijndaelManaged, mscorlib";
                break;
            default: //case "Triple DES":
                fullEncryptionName = "System.Security.Cryptography.TripleDESCryptoServiceProvider, mscorlib";
        }
        return fullEncryptionName;
    }

    private showFsCreationAdvancedStepsIfNecessary(filesystemName: string, bundles: string[], settings: {}) {
        var securedSettings = {};
        var savedKey: string;

        var encryptionDeferred = $.Deferred();

        if (bundles.contains("Encryption")) {
            var createEncryptionViewModel = new createEncryption();
            createEncryptionViewModel
                .creationEncryption
                .done((key: string, encryptionAlgorithm: string, encryptionBits: string, isEncryptedIndexes: string) => {
                    savedKey = key;
                    securedSettings = {
                        'Raven/Encryption/Key': key,
                        'Raven/Encryption/Algorithm': this.getEncryptionAlgorithmFullName(encryptionAlgorithm),
                        'Raven/Encryption/KeyBitsPreference': encryptionBits,
                        'Raven/Encryption/EncryptIndexes': isEncryptedIndexes
                    };
                    encryptionDeferred.resolve(securedSettings);
                });
            app.showDialog(createEncryptionViewModel);
        } else {
            encryptionDeferred.resolve();
        }

        encryptionDeferred.done(() => {
            new createFilesystemCommand(filesystemName, settings, securedSettings)
                .execute()
                .done(() => {
                    var newFileSystem = this.addNewFileSystem(filesystemName, bundles);
                    this.selectResource(newFileSystem);

                    var encryptionConfirmationDialogPromise = $.Deferred();
                    if (!jQuery.isEmptyObject(securedSettings)) {
                            var createEncryptionConfirmationViewModel = new createEncryptionConfirmation(savedKey);
                            createEncryptionConfirmationViewModel.dialogPromise.done(() => encryptionConfirmationDialogPromise.resolve());
                            createEncryptionConfirmationViewModel.dialogPromise.fail(() => encryptionConfirmationDialogPromise.reject());
                            app.showDialog(createEncryptionConfirmationViewModel);
                    } else {
                        encryptionConfirmationDialogPromise.resolve();
                    }

                    this.createDefaultFilesystemSettings(newFileSystem, bundles).always(() => {
                        if (bundles.contains("Versioning")) {
                            encryptionConfirmationDialogPromise.always(() => {
                                var settingsDialog = new filesystemSettingsDialog(bundles);
                                app.showDialog(settingsDialog);
                            });
                        }
                    });
                });
        });
    }

    private addNewFileSystem(fileSystemName: string, bundles: string[]): fileSystem {
        var foundFileSystem = this.fileSystems.first((fs: fileSystem) => fs.name === fileSystemName);

        if (!foundFileSystem) {
            var newFileSystem = new fileSystem(fileSystemName, true, false, false, bundles);
            this.fileSystems.unshift(newFileSystem);
            this.filterResources();
            return newFileSystem;
        }
        return foundFileSystem;
    }

    private showCsCreationAdvancedStepsIfNecessary(counterStorageName: string, bundles: string[], settings: {}) {
        new createCounterStorageCommand(counterStorageName, settings)
            .execute()
            .done(() => {
                var newCounterStorage = this.addNewCounterStorage(counterStorageName, bundles);
                this.selectResource(newCounterStorage);
            });
    }

    private addNewCounterStorage(counterStorageName: string, bundles: string[]): counterStorage {
        var foundCounterStorage = this.counterStorages.first((cs: counterStorage) => cs.name === counterStorageName);
        if (!!foundCounterStorage)
            return foundCounterStorage;

        var newCounterStorage = new counterStorage(counterStorageName, true, false, bundles);
        this.counterStorages.unshift(newCounterStorage);
        this.filterResources();
        return newCounterStorage;
    }

    private showTsCreationAdvancedStepsIfNecessary(timeSeriesName: string, bundles: string[], settings: {}) {
        new createTimeSeriesCommand(timeSeriesName, settings)
            .execute()
            .done(() => {
            var newTimeSeries = this.addNewTimeSeries(timeSeriesName, bundles);
            this.selectResource(newTimeSeries);
        });
    }

    private addNewTimeSeries(timeSeriesName: string, bundles: string[]): timeSeries {
        var foundTimeSeries = this.timeSeries.first((ts: timeSeries) => ts.name === timeSeriesName);
        if (!!foundTimeSeries)
            return foundTimeSeries;

        var newTimeSeries = new timeSeries(timeSeriesName, true, false, bundles);
        this.timeSeries.unshift(newTimeSeries);
        this.filterResources();
        return newTimeSeries;
    }
}

export = resources;

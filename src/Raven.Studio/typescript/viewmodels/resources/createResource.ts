import app = require("durandal/app");
import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import createDatabase = require("viewmodels/resources/createDatabase");
import createFileSystem = require("viewmodels/resources/createFilesystem");
import createCounterStorage = require("viewmodels/resources/createCounterStorage");
import createTimeSeries = require("viewmodels/resources/createTimeSeries");
import database = require("models/resources/database");
import resource = require("models/resources/resource");
import fileSystem = require("models/filesystem/filesystem");
import counterStorage = require("models/counter/counterStorage");
import timeSeries = require("models/timeSeries/timeSeries");
import shell = require("viewmodels/shell");
import EVENTS = require("common/constants/events");
import resourcesManager = require("common/shell/resourcesManager");

import createEncryption = require("viewmodels/resources/createEncryption");
import createDatabaseCommand = require("commands/resources/createDatabaseCommand");
import createEncryptionConfirmation = require("viewmodels/resources/createEncryptionConfirmation");
import databaseSettingsDialog = require("viewmodels/resources/databaseSettingsDialog");
import createDefaultDbSettingsCommand = require("commands/resources/createDefaultSettingsCommand");

import filesystemSettingsDialog = require("viewmodels/resources/filesystemSettingsDialog");

import createDefaultFsSettingsCommand = require("commands/filesystem/createDefaultSettingsCommand");
import createFilesystemCommand = require("commands/filesystem/createFilesystemCommand");
import createCounterStorageCommand = require("commands/resources/createCounterStorageCommand");
import createTimeSeriesCommand = require("commands/resources/createTimeSeriesCommand");
import license = require("models/auth/license");

class createResource extends dialogViewModelBase {
    databaseType = database.type;
    fileSystemType = fileSystem.type;
    counterStorageType = counterStorage.type;
    timeSeriesType = timeSeries.type;
    resourceType = ko.observable<string>("Database");
    createDatabasePart: createDatabase;
    createFileSystemPart: createFileSystem;
    createCounterStoragePart: createCounterStorage;
    createTimeSeriesPart: createTimeSeries;

    resourceTypes = ko.observableArray([
        { resourceType: this.databaseType, title: "Database", iconName: "fa fa-database fa-2x" },
        { resourceType: this.fileSystemType, title: "File System", iconName: "fa fa-file-image-o fa-2x" },
        { resourceType: this.counterStorageType, title: "Counter Storage", iconName: "fa fa-sort-numeric-desc fa-2x", experimental: false },
        { resourceType: this.timeSeriesType, title: "Time Series", iconName: "fa fa-clock-o fa-2x", experimental: false }
    ]);
    checkedResource = ko.observable<string>(this.databaseType);

    constructor() {
        super();

        this.createDatabasePart = new createDatabase(this);
        this.createFileSystemPart = new createFileSystem(this);
        this.createCounterStoragePart = new createCounterStorage(this);
        this.createTimeSeriesPart = new createTimeSeries(this);
        this.checkedResource.subscribe((resourceType: string) => {
            var resourceTypeText = "";
            switch (resourceType) {
                case this.databaseType:
                    this.enableDbTab();
                    resourceTypeText = "Database";
                    break;
                case this.fileSystemType:
                    this.enableFsTab();
                    resourceTypeText = "File System";
                    break;
                case this.counterStorageType:
                    this.enableCsTab();
                    resourceTypeText = "Counter Storage";
                    break;
                case this.timeSeriesType:
                    this.enableTsTab();
                    resourceTypeText = "Time Series";
                    break;
                default:
                    break;
            }
            this.resourceType(resourceTypeText);
        });

        this.initPostActions();
    }

    private initPostActions() {
        this.createDatabasePart
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

        this.createFileSystemPart
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

        this.createCounterStoragePart
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

        this.createTimeSeriesPart
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
    }

    private showDbCreationAdvancedStepsIfNecessary(databaseName: string, bundles: string[], settings: dictionary<string>, clusterWide: boolean) {//TODO: extract to different view
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
            app.showBootstrapDialog(createEncryptionViewModel);
        } else {
            encryptionDeferred.resolve();
        }

        encryptionDeferred.done(() => {
            new createDatabaseCommand(databaseName, settings, securedSettings)
                .execute()
                .done(() => {
                    this.addNewDatabase(databaseName, bundles, clusterWide);
                    //TODO: this.selectResource(newDatabase);

                    var encryptionConfirmationDialogPromise = $.Deferred();
                    if (!jQuery.isEmptyObject(securedSettings)) {
                        var createEncryptionConfirmationViewModel = new createEncryptionConfirmation(savedKey);
                        createEncryptionConfirmationViewModel.dialogPromise.done(() => encryptionConfirmationDialogPromise.resolve());
                        createEncryptionConfirmationViewModel.dialogPromise.fail(() => encryptionConfirmationDialogPromise.reject());
                        app.showBootstrapDialog(createEncryptionConfirmationViewModel);
                    } else {
                        encryptionConfirmationDialogPromise.resolve();
                    }

                    /* TODO:
                    this.createDefaultDatabaseSettings(newDatabase, bundles).always(() => {
                        if (bundles.contains("Quotas") || bundles.contains("Versioning") || bundles.contains("SqlReplication")) {
                            encryptionConfirmationDialogPromise.always(() => {
                                // schedule dialog using setTimeout to avoid issue with dialog width
                                // (it isn't recalculated when dialog is already opened)
                                setTimeout(() => {
                                    var settingsDialog = new databaseSettingsDialog(bundles);
                                    app.showBootstrapDialog(settingsDialog);
                                }, 1);
                            });
                        }
                    });*/
                });
        });
    }

    private addNewDatabase(databaseName: string, bundles: string[], clusterWide: boolean): void {
        ko.postbox.publish(EVENTS.Resource.Created, //TODO: it might be temporary event as we use changes api for notifications about newly created resources. 
        {
            qualifier: database.qualifier,
            name: databaseName
        } as resourceCreatedEventArgs);

        /* TODO
        var foundDatabase = this.databases.first((db: database) => db.name == databaseName);

        if (!foundDatabase) {
            var newDatabase = new database(databaseName, true, false, bundles, undefined, undefined, clusterWide);
        //TODO: use resources manager to get instance of database object 
            this.databases.unshift(newDatabase);
            this.filterResources();
            return newDatabase;
        }
        return foundDatabase;*/
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
            app.showBootstrapDialog(createEncryptionViewModel);
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
                        app.showBootstrapDialog(createEncryptionConfirmationViewModel);
                    } else {
                        encryptionConfirmationDialogPromise.resolve();
                    }

                    this.createDefaultFilesystemSettings(newFileSystem, bundles).always(() => {
                        if (bundles.contains("Versioning")) {
                            encryptionConfirmationDialogPromise.always(() => {
                                var settingsDialog = new filesystemSettingsDialog(bundles);
                                app.showBootstrapDialog(settingsDialog);
                            });
                        }
                    });
                });
        });
    }

    private addNewFileSystem(fileSystemName: string, bundles: string[]): fileSystem {
        return null; //TODO
        /* TODO:
        var foundFileSystem = this.fileSystems.first((fs: fileSystem) => fs.name === fileSystemName);

        if (!foundFileSystem) {
            var newFileSystem = new fileSystem(fileSystemName, true, false, false, bundles);
            this.fileSystems.unshift(newFileSystem);
            this.filterResources();
            return newFileSystem;
        }
        return foundFileSystem;*/
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
        return null;// TODO:
        /*
        var foundCounterStorage = this.counterStorages.first((cs: counterStorage) => cs.name === counterStorageName);
        if (!!foundCounterStorage)
            return foundCounterStorage;

        var newCounterStorage = new counterStorage(counterStorageName, true, false, bundles);
        this.counterStorages.unshift(newCounterStorage);
        this.filterResources();
        return newCounterStorage;*/
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
        return null; //TODO:
        /* TODO
        var foundTimeSeries = this.timeSeries.first((ts: timeSeries) => ts.name === timeSeriesName);
        if (!!foundTimeSeries)
            return foundTimeSeries;

        var newTimeSeries = new timeSeries(timeSeriesName, true, false, bundles);
        this.timeSeries.unshift(newTimeSeries);
        this.filterResources();
        return newTimeSeries;*/
    }

    compositionComplete() {
        this.enableDbTab();
    }

    private enableDbTab() {
        this.alterFormControls("#dbContainer", false);
        this.alterFormControls("#fsContainer", true);
        this.alterFormControls("#csContainer", true);
        this.alterFormControls("#tsContainer", true);
    }

    private enableFsTab() {
        this.alterFormControls("#dbContainer", true);
        this.alterFormControls("#fsContainer", false);
        this.alterFormControls("#csContainer", true);
        this.alterFormControls("#tsContainer", true);
    }

    private enableCsTab() {
        this.alterFormControls("#dbContainer", true);
        this.alterFormControls("#fsContainer", true);
        this.alterFormControls("#csContainer", false);
        this.alterFormControls("#tsContainer", true);
    }

    private enableTsTab() {
        this.alterFormControls("#dbContainer", true);
        this.alterFormControls("#fsContainer", true);
        this.alterFormControls("#csContainer", true);
        this.alterFormControls("#tsContainer", false);
    }

    private alterFormControls(formSelector: string, disabled: boolean) {
        $(formSelector + " input").prop("disabled", disabled);
        $(formSelector + " select").prop("disabled", disabled);
    }

    cancel() {
        dialog.close(this);
    }

    nextOrCreate() {
        switch (this.checkedResource()) {
            case this.databaseType:
                this.createDatabasePart.nextOrCreate();
                break;
            case this.fileSystemType:
                this.createFileSystemPart.nextOrCreate();
                break;
            case this.counterStorageType:
                this.createCounterStoragePart.nextOrCreate();
                break;
            case this.timeSeriesType:
                this.createTimeSeriesPart.nextOrCreate();
                break;
            default:
                throw "Can't figure what to do!";
        }
    }

    selectResource(rs: resource, activateResource: boolean = true) {
        /* TODO:
        if (this.optionsClicked() === false) {
            if (activateResource) {
                rs.activate();
            }
            this.selectedResource(rs);
        }

        this.optionsClicked(false);*/
    }
}

export = createResource;

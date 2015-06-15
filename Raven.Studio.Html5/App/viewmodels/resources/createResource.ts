import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import createDatabase = require("viewmodels/resources/createDatabase");
import createFileSystem = require("viewmodels/resources/createFilesystem");
import createCounterStorage = require("viewmodels/resources/createCounterStorage");
import createTimeSeries = require("viewmodels/resources/createTimeSeries");
import license = require("models/auth/license");
import database = require("models/resources/database");
import fileSystem = require("models/filesystem/filesystem");
import counterStorage = require("models/counter/counterStorage");
import timeSeries = require("models/timeSeries/timeSeries");
import shell = require("viewmodels/shell");

class createResource extends dialogViewModelBase {
    databaseType = database.type;
    fileSystemType = fileSystem.type;
    counterStorageType = counterStorage.type;
    timeSeriesType = timeSeries.type;
    resourceType = ko.observable<string>(this.databaseType);
    resourceTypes = [
        { value: database.type, name: "Database" }, 
        { value: fileSystem.type, name: "File System" }, 
        { value: counterStorage.type, name: "Counter Storage" },
        { value: timeSeries.type, name: "Time Series" }
    ];
    createDatabasePart: createDatabase;
    createFileSystemPart: createFileSystem;
    createCounterStoragePart: createCounterStorage;
    createTimeSeriesPart: createTimeSeries;

    constructor(/*databases: KnockoutObservableArray<database>, filesystems: KnockoutObservableArray<fileSystem>, licenseStatus: KnockoutObservable<licenseStatusDto>*/) {
        super();
        this.createDatabasePart = new createDatabase(shell.databases, license.licenseStatus, this);
        this.createFileSystemPart = new createFileSystem(shell.fileSystems, license.licenseStatus, this);
        this.createCounterStoragePart = new createCounterStorage(shell.counterStorages, license.licenseStatus, this);
        this.createTimeSeriesPart = new createTimeSeries(shell.timeSeries, license.licenseStatus, this);
        this.resourceType.subscribe((resourceType: string) => {
            switch (resourceType) {
                case this.databaseType:
                    this.enableDbTab();
                    break;
                case this.fileSystemType:
                    this.enableFsTab();
                    break;
                case this.counterStorageType:
                    this.enableCsTab();
                    break;
                case this.timeSeriesType:
                    this.enableTsTab();
                    break;
                default:
                    break;
            }
        });
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
        switch (this.resourceType()) {
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
}

export = createResource;

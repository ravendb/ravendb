import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import createDatabase = require("viewmodels/resources/createDatabase");
import createFileSystem = require("viewmodels/resources/createFilesystem");
import createCounterStorage = require("viewmodels/resources/createCounterStorage");
import createTimeSeries = require("viewmodels/resources/createTimeSeries");
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
}

export = createResource;

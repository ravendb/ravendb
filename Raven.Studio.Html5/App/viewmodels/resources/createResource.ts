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
    resourceType = ko.observable<string>("Database");
    createDatabasePart: createDatabase;
    createFileSystemPart: createFileSystem;
    createCounterStoragePart: createCounterStorage;
    createTimeSeriesPart: createTimeSeries;

    resourceTypes = ko.observableArray([
        { resourceType: database.type, title: "Database", iconName: "fa fa-database fa-2x", experimental: false },
        { resourceType: fileSystem.type, title: "File System", iconName: "fa fa-file-image-o fa-2x", experimental: false },
        { resourceType: counterStorage.type, title: "Counter Storage", iconName: "fa fa-sort-numeric-desc fa-2x", experimental: false },
        { resourceType: timeSeries.type, title: "Time Series", iconName: "fa fa-clock-o fa-2x", experimental: false }
    ]);
    checkedResource = ko.observable<string>(database.type);

    constructor() {
        super();

		if (!shell.has40Features()) {
			this.resourceTypes().first(r => r.resourceType === counterStorage.type).experimental = true;
			this.resourceTypes().first(r => r.resourceType === timeSeries.type).experimental = true;
		}


        this.createDatabasePart = new createDatabase(this);
        this.createFileSystemPart = new createFileSystem(this);
        this.createCounterStoragePart = new createCounterStorage(this);
        this.createTimeSeriesPart = new createTimeSeries(this);
        this.checkedResource.subscribe((resourceType: string) => {
            var resourceTypeText = "";
            switch (resourceType) {
                case database.type:
                    this.enableDbTab();
                    resourceTypeText = "Database";
                    break;
                case fileSystem.type:
                    this.enableFsTab();
                    resourceTypeText = "File System";
                    break;
                case counterStorage.type:
                    this.enableCsTab();
                    resourceTypeText = "Counter Storage";
                    break;
                case timeSeries.type:
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
            case database.type:
                this.createDatabasePart.nextOrCreate();
                break;
            case fileSystem.type:
                this.createFileSystemPart.nextOrCreate();
                break;
            case counterStorage.type:
                this.createCounterStoragePart.nextOrCreate();
                break;
            case timeSeries.type:
                this.createTimeSeriesPart.nextOrCreate();
                break;
            default:
                throw "Can't figure what to do!";
        }
    }
}

export = createResource;

import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import createDatabase = require("viewmodels/resources/createDatabase");
import createFileSystem = require("viewmodels/resources/createFilesystem");
import createCounterStorage = require("viewmodels/resources/createCounterStorage");
import license = require("models/auth/license");
import database = require("models/resources/database");
import fileSystem = require("models/filesystem/filesystem");
import counterStorage = require("models/counter/counterStorage");
import shell = require("viewmodels/shell");

class createResource extends dialogViewModelBase {
    databaseType = database.type;
    fileSystemType = fileSystem.type;
    counterStorageType = counterStorage.type;
    resourceType = ko.observable<string>(this.databaseType);
    resourceTypes = [
        { value: database.type, name: "Database" }, 
        { value: fileSystem.type, name: "File System" }, 
        { value: counterStorage.type, name: "Counter Storage" }
    ];
    createDatabasePart: createDatabase;
    createFileSystemPart: createFileSystem;
    createCounterStoragePart: createCounterStorage;

    constructor(/*databases: KnockoutObservableArray<database>, filesystems: KnockoutObservableArray<fileSystem>, licenseStatus: KnockoutObservable<licenseStatusDto>*/) {
        super();
        this.createDatabasePart = new createDatabase(shell.databases, license.licenseStatus, this);
        this.createFileSystemPart = new createFileSystem(shell.fileSystems, license.licenseStatus, this);
        this.createCounterStoragePart = new createCounterStorage(shell.counterStorages, license.licenseStatus, this);
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
    }

    private enableFsTab() {
        this.alterFormControls("#dbContainer", true);
        this.alterFormControls("#fsContainer", false);
        this.alterFormControls("#csContainer", true);
    }

    private enableCsTab() {
        this.alterFormControls("#dbContainer", true);
        this.alterFormControls("#fsContainer", true);
        this.alterFormControls("#csContainer", false);
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
            default:
                throw "Can't figure what to do!";
        }
    }
}

export = createResource;

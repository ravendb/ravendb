import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import createDatabase = require("viewmodels/resources/createDatabase");
import createFilesystem = require("viewmodels/resources/createFilesystem");
import database = require("models/resources/database");
import filesystem = require("models/filesystem/filesystem");

class createResource extends dialogViewModelBase {

    resourceType = ko.observable<string>('db');

    createDatabasePart: createDatabase;
    createFilesystemPart: createFilesystem;

    constructor(databases: KnockoutObservableArray<database>, filesystems: KnockoutObservableArray<filesystem>, licenseStatus: KnockoutObservable<licenseStatusDto>) {
        super();
        this.createDatabasePart = new createDatabase(databases, licenseStatus, this);
        this.createFilesystemPart = new createFilesystem(filesystems, licenseStatus, this);
        this.resourceType.subscribe(v => v == "db" ? this.enableDbTab() : this.enableFsTab());
    }

    compositionComplete() {
        this.enableDbTab();
    }

    private enableDbTab() {
        this.alterFormControls("#dbContainer", false);
        this.alterFormControls("#fsContainer", true);
    }

    private enableFsTab() {
        this.alterFormControls("#dbContainer", true);
        this.alterFormControls("#fsContainer", false);
    }

    private alterFormControls(formSelector: string, disabled: boolean) {
        $(formSelector + " input").prop('disabled', disabled);
        $(formSelector + " select").prop('disabled', disabled);
    }


    cancel() {
        dialog.close(this);
    }

    nextOrCreate() {
        if (this.resourceType() == 'db') {
            this.createDatabasePart.nextOrCreate();
        } else {
            this.createFilesystemPart.nextOrCreate();
        }
    }
   
}

export = createResource;

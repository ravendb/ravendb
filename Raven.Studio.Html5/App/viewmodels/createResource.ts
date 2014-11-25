import dialog = require("plugins/dialog");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import createDatabase = require("viewmodels/createDatabase");
import createFilesystem = require("viewmodels/filesystem/createFilesystem");
import database = require("models/database");
import filesystem = require("models/filesystem/filesystem");

class createResource extends dialogViewModelBase {

    resourceType = ko.observable<string>('db');

    createDatabasePart: createDatabase;
    createFilesystemPart: createFilesystem;

    constructor(databases: KnockoutObservableArray<database>, filesystems: KnockoutObservableArray<filesystem>, licenseStatus: KnockoutObservable<licenseStatusDto>) {
        super();
        this.createDatabasePart = new createDatabase(databases, licenseStatus, this);
        this.createFilesystemPart = new createFilesystem(filesystems, this);
    }

    cancel() {
        dialog.close(this);
    }

    submitForm() {
        if (this.resourceType() == 'db') {
            $("#createDbForm").submit();
        } else {
            $("#createFsForm").submit();
        }
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

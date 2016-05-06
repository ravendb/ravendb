import viewModelBase = require("viewmodels/viewModelBase");
import shell = require("viewmodels/shell");
import resource = require("models/resources/resource");
import database = require("models/resources/database");
import filesystem = require("models/filesystem/filesystem");
import counterStorage = require("models/counter/counterStorage");
import backupDatabaseCommand = require("commands/maintenance/backupDatabaseCommand");
import backupFilesystemCommand = require("commands/filesystem/backupFilesystemCommand");
import backupCounterStorageCommand = require("commands/counter/backupCounterStorageCommand");
import getResourceDrives = require("commands/resources/getResourceDrives");

class resourceBackup {
    incremental = ko.observable<boolean>(false);
    resourceName = ko.observable<string>('');
    backupLocation = ko.observable<string>('');
    backupStatusMessages = ko.observableArray<backupMessageDto>();
    isBusy = ko.observable<boolean>(); 
    resourcesNames: KnockoutComputed<string[]>;
    fullTypeName: KnockoutComputed<string>;
    searchResults: KnockoutComputed<string[]>;
    nameCustomValidityError: KnockoutComputed<string>;
    keepDown = ko.observable<boolean>(false);
    
    drivesForCurrentResource = ko.observable<string[]>([]);
    displaySameDriveWarning = ko.computed(() => {
        var resourceDrives = this.drivesForCurrentResource();
        var location = this.backupLocation().toLowerCase().substr(0, 3);
        return resourceDrives.indexOf(location) !== -1;
    }); 

    has40Features = ko.computed(() => shell.has40Features());

    constructor(private type: TenantType, private resources: KnockoutObservableArray<resource>) {
        this.resourcesNames = ko.computed(() => resources().map((rs: resource) => rs.name));

        this.fullTypeName = ko.computed(() => {
            var rs = resources();

            if (rs.length > 0) {
                return rs[0].fullTypeName;
            }
            return null;
        });

        this.searchResults = ko.computed(() => {
            var newResourceName = this.resourceName();
            return this.resourcesNames().filter((name) => name.toLowerCase().indexOf(newResourceName.toLowerCase()) > -1);
        });

        this.nameCustomValidityError = ko.computed(() => {
            var errorMessage: string = "";
            var newResourceName = this.resourceName();
            var foundRs = this.resources().first((rs: resource) => newResourceName === rs.name && rs.type === this.type);

            if (!foundRs && newResourceName.length > 0) {
                errorMessage = this.fullTypeName() + " name doesn't exist!";
            }

            return errorMessage;
        });

        this.resourceName.throttle(200).subscribe((resource) => {
            var foundRs = this.resources().first((rs: resource) => resource === rs.name && rs.type === this.type);
            if (foundRs) {
                new getResourceDrives(foundRs.name, foundRs.type.toString()).execute()
                    .done((drives: string[]) => {
                        this.drivesForCurrentResource(drives);
                    });
            } else {
                this.drivesForCurrentResource([]);
            }
        });
    }

    toggleKeepDown() {
        this.keepDown.toggle();
        this.forceKeepDown();
    }

    forceKeepDown() {
        if (this.keepDown()) {
            var body = document.getElementsByTagName("body")[0];
            body.scrollTop = body.scrollHeight;
        }
    }

    updateBackupStatus(newBackupStatus: backupStatusDto) {
        this.backupStatusMessages(newBackupStatus.Messages);
        this.isBusy(newBackupStatus.IsRunning);
        this.forceKeepDown();
    }
}

class backupDatabase extends viewModelBase {

    private dbBackupOptions = new resourceBackup(TenantType.Database, shell.databases);
    private fsBackupOptions = new resourceBackup(TenantType.FileSystem, shell.fileSystems);
    private csBackupOptions = new resourceBackup(TenantType.CounterStorage, shell.counterStorages);
    
    isForbidden = ko.observable<boolean>();

    canActivate(args): any {
        this.isForbidden(!shell.isGlobalAdmin());
        return true;
    }

    activate(args) {
        super.activate(args);
        this.updateHelpLink("FT7RV6");
    }

    compositionComplete() {
        super.compositionComplete();
        $('form :input[name="databaseName"]').on("keypress", (e) => e.which !== 13);
        $('form :input[name="filesystemName"]').on("keypress", (e) => e.which !== 13);
    }

    startDbBackup() {
        var backupOptions = this.dbBackupOptions;
        backupOptions.isBusy(true);

        var dbToBackup = shell.databases.first((db: database) => db.name === backupOptions.resourceName());
        new backupDatabaseCommand(dbToBackup, backupOptions.backupLocation(), backupOptions.updateBackupStatus.bind(this.dbBackupOptions), backupOptions.incremental())
            .execute()
            .always(() => backupOptions.isBusy(false));
    }

    startFsBackup() {
        var backupOptions = this.fsBackupOptions;
        backupOptions.isBusy(true);

        var fsToBackup = shell.fileSystems.first((fs: filesystem) => fs.name === backupOptions.resourceName());
        new backupFilesystemCommand(fsToBackup, backupOptions.backupLocation(), backupOptions.updateBackupStatus.bind(this.fsBackupOptions), backupOptions.incremental())
            .execute()
            .always(() => backupOptions.isBusy(false));
    }

    startCsBackup() {
        var backupOptions = this.csBackupOptions;
        backupOptions.isBusy(true);

        var csToBackup = shell.counterStorages.first((cs: counterStorage) => cs.name === backupOptions.resourceName());
        new backupCounterStorageCommand(csToBackup, backupOptions.backupLocation(), backupOptions.updateBackupStatus.bind(this.csBackupOptions), backupOptions.incremental())
            .execute()
            .always(() => backupOptions.isBusy(false));
    }
}

export = backupDatabase;

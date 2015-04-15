import viewModelBase = require("viewmodels/viewModelBase");
import shell = require("viewmodels/shell");
import database = require("models/resources/database");
import filesystem = require("models/filesystem/filesystem");
import resource = require("models/resources/resource");
import backupDatabaseCommand = require("commands/maintenance/backupDatabaseCommand");
import backupFilesystemCommand = require("commands/filesystem/backupFilesystemCommand");

class resourceBackup {
    incremental = ko.observable<boolean>(false);
    resourceName = ko.observable<string>('');
    backupLocation = ko.observable<string>('');
    backupStatusMessages = ko.observableArray<backupMessageDto>();
    isBusy = ko.observable<boolean>(); 
    resourcesNames: KnockoutComputed<string[]>;
    searchResults: KnockoutComputed<string[]>;
    nameCustomValidityError: KnockoutComputed<string>;

    constructor(private type: string, private resources: KnockoutObservableArray<resource>) {
        this.resourcesNames = ko.computed(() => resources().map((rs: resource) => rs.name));
        this.searchResults = ko.computed(() => {
            var newResourceName = this.resourceName();
            return this.resourcesNames().filter((name) => name.toLowerCase().indexOf(newResourceName.toLowerCase()) > -1);
        });

        this.nameCustomValidityError = ko.computed(() => {
            var errorMessage: string = '';
            var newResourceName = this.resourceName();
            var foundRs = this.resources().first((rs: resource) => newResourceName == rs.name && rs.type == this.type);

            if (!foundRs && newResourceName.length > 0) {
                errorMessage = (this.type == database.type ? "Database" : "File system") + " name doesn't exist!";
            }

            return errorMessage;
        });
    }
}

class backupDatabase extends viewModelBase {

    private dbBackupOptions = new resourceBackup(database.type, shell.databases);
    private fsBackupOptions = new resourceBackup(filesystem.type, shell.fileSystems);
    
    canActivate(args): any {
        return true;
    }

    activate(args) {
        super.activate(args);
        this.updateHelpLink('FT7RV6');
    }

    compositionComplete() {
        super.compositionComplete();
        $('form :input[name="databaseName"]').on("keypress", (e) => e.which != 13);
        $('form :input[name="filesystemName"]').on("keypress", (e) => e.which != 13);
    }

    startDbBackup() {
        this.dbBackupOptions.isBusy(true);

        var updateBackupStatus = (newBackupStatus: backupStatusDto) => {
            this.dbBackupOptions.backupStatusMessages(newBackupStatus.Messages);
            this.dbBackupOptions.isBusy(!!newBackupStatus.IsRunning);
        };

        var dbToBackup = shell.databases.first((db: database) => db.name == this.dbBackupOptions.resourceName());
        new backupDatabaseCommand(dbToBackup, this.dbBackupOptions.backupLocation(), updateBackupStatus, this.dbBackupOptions.incremental())
            .execute()
            .always(() => this.dbBackupOptions.isBusy(false));
    }

    startFsBackup() {
        this.fsBackupOptions.isBusy(true);

        var updateBackupStatus = (newBackupStatus: backupStatusDto) => {
            this.fsBackupOptions.backupStatusMessages(newBackupStatus.Messages);
            this.fsBackupOptions.isBusy(!!newBackupStatus.IsRunning);
        };

        var fsToBackup = shell.fileSystems.first((fs: filesystem) => fs.name == this.fsBackupOptions.resourceName());
        new backupFilesystemCommand(fsToBackup, this.fsBackupOptions.backupLocation(), updateBackupStatus, this.fsBackupOptions.incremental())
            .execute()
            .always(() => this.fsBackupOptions.isBusy(false));
    }
}

export = backupDatabase;
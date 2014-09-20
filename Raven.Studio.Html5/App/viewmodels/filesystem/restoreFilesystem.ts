import viewModelBase = require("viewmodels/viewModelBase");
import shell = require("viewmodels/shell");
import database = require("models/database");
import filesystem = require("models/filesystem/filesystem");

class restoreFilesystem extends viewModelBase {

    defrag = ko.observable<boolean>(false);
    backupLocation = ko.observable<string>('');
    filesystemLocation = ko.observable<string>();
    filesystemName = ko.observable<string>();
    nameCustomValidityError: KnockoutComputed<string>;

    restoreStatusMessages = ko.observableArray<string>();
    isBusy = ko.observable<boolean>();

    constructor() {
        super();

        this.nameCustomValidityError = ko.computed(() => {
            var errorMessage: string = '';
            var newFilesystemName = this.filesystemName();
            var foundFs = shell.fileSystems.first((fs: filesystem) => newFilesystemName == fs.name);

            if (!!foundFs && newFilesystemName.length > 0) {
                errorMessage = "Filesystem name already exists!";
            }

            return errorMessage;
        });
    }

    canActivate(args): any {
        return true;
    }

    startRestore() {
        this.isBusy(true);

        var restoreFilesystemDto: filesystemRestoreRequestDto = {
            BackupLocation: this.backupLocation(),
            FilesystemLocation: this.filesystemLocation(),
            FilesystemName: this.filesystemName()
        };
        var updateRestoreStatus = (newRestoreStatus: restoreStatusDto) => {
            this.restoreStatusMessages(newRestoreStatus.Messages);
            this.isBusy(!!newRestoreStatus.IsRunning);
        };

        require(["commands/filesystem/startRestoreCommand"], startRestoreCommand => {
            new startRestoreCommand(this.defrag(), restoreFilesystemDto, updateRestoreStatus)
                .execute();
        });
    }
}

export = restoreFilesystem;  
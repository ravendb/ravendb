import viewModelBase = require("viewmodels/viewModelBase");
import shell = require("viewmodels/shell");
import database = require("models/database");
import filesystem = require("models/filesystem/filesystem");
import getDocumentWithMetadataCommand = require("commands/getDocumentWithMetadataCommand");
import appUrl = require("common/appUrl");
import monitorRestoreCommand = require("commands/filesystem/monitorRestoreCommand");

class restoreFilesystem extends viewModelBase {

    defrag = ko.observable<boolean>(false);
    backupLocation = ko.observable<string>('');
    filesystemLocation = ko.observable<string>();
    filesystemName = ko.observable<string>();
    nameCustomValidityError: KnockoutComputed<string>;

    restoreStatusMessages = ko.observableArray<string>();
    isBusy = ko.observable<boolean>();
    anotherRestoreInProgres = ko.observable<boolean>(false);
    keepDown = ko.observable<boolean>(false);

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
        this.isBusy(true);
        var deferred = $.Deferred();
        var db = appUrl.getSystemDatabase();
        var self = this;

        new getDocumentWithMetadataCommand("Raven/Restore/InProgress", db, true).execute()
            .fail(() => deferred.resolve({ redirect: appUrl.forSettings(db) }))
            .done((result) => {
                if (result) {
                    // looks like another restore is in progress
                    this.anotherRestoreInProgres(true);
                    new monitorRestoreCommand($.Deferred(), result.Resource, this.updateRestoreStatus.bind(self))
                        .execute()
                        .always(() => {
                            $("#rawLogsContainer").resize();
                            this.anotherRestoreInProgres(false);
                        });

                } else {
                    this.isBusy(false);
                }
                deferred.resolve({ can: true })
            });

        return deferred;
    }

    private updateRestoreStatus(newRestoreStatus: restoreStatusDto) {
        this.restoreStatusMessages(newRestoreStatus.Messages);
        if (this.keepDown()) {
            var logsPre = document.getElementById('restoreLogPre');
            logsPre.scrollTop = logsPre.scrollHeight;
        }
        this.isBusy(!!newRestoreStatus.IsRunning);
    }

    toggleKeepDown() {
        this.keepDown.toggle();
        if (this.keepDown() == true) {
            var logsPre = document.getElementById('restoreLogPre');
            logsPre.scrollTop = logsPre.scrollHeight;
        }
    }

    startRestore() {
        this.isBusy(true);
        var self = this;

        var restoreFilesystemDto: filesystemRestoreRequestDto = {
            BackupLocation: this.backupLocation(),
            FilesystemLocation: this.filesystemLocation(),
            FilesystemName: this.filesystemName()
        };

        require(["commands/filesystem/startRestoreCommand"], startRestoreCommand => {
            new startRestoreCommand(this.defrag(), restoreFilesystemDto, self.updateRestoreStatus.bind(self))
                .execute();
        });
    }
}

export = restoreFilesystem;  
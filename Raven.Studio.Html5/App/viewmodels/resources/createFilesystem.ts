import viewModelBase = require("viewmodels/viewModelBase");
import dialog = require("plugins/dialog");
import appUrl = require("common/appUrl");
import filesystem = require("models/filesystem/filesystem");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import getDatabaseStatsCommand = require("commands/resources/getDatabaseStatsCommand");
import getStatusDebugConfigCommand = require("commands/database/debug/getStatusDebugConfigCommand");

class createFilesystem extends viewModelBase {

    public creationTask = $.Deferred();
    creationTaskStarted = false;

    public fileSystemName = ko.observable('');
    nameCustomValidityError: KnockoutComputed<string>;
    fileSystemPath = ko.observable('');
    pathCustomValidityError: KnockoutComputed<string>;
    fileSystemLogsPath = ko.observable('');
    storageEngine = ko.observable('');

    logsCustomValidityError: KnockoutComputed<string>;

    isEncryptionBundleEnabled = ko.observable(false);
    isVersioningBundleEnabled = ko.observable(false);

	allowVoron = ko.observable<boolean>(true);
	voronWarningVisible = ko.computed(() => !this.allowVoron() && "voron" === this.storageEngine());

    constructor(private filesystems: KnockoutObservableArray<filesystem>, private licenseStatus: KnockoutObservable<licenseStatusDto>, private parent: dialogViewModelBase) {
        super();

        this.nameCustomValidityError = ko.computed(() => {
            var errorMessage: string = '';
            var newFileSystemName = this.fileSystemName();

            if (this.isFilesystemNameExists(newFileSystemName, this.filesystems()) == true) {
                errorMessage = "File system name already exists!";
            }
            else if ((errorMessage = this.checkName(newFileSystemName)) != '') { }

            return errorMessage;
        });

        this.pathCustomValidityError = ko.computed(() => {
            var newPath = this.fileSystemPath();
            var errorMessage: string = this.isPathLegal(newPath, "Path");
            return errorMessage;
        });

        this.logsCustomValidityError = ko.computed(() => {
            var newPath = this.fileSystemLogsPath();
            var errorMessage: string = this.isPathLegal(newPath, "Logs");
            return errorMessage;
        });

		this.fetchAllowVoron();
    }

	fetchAllowVoron() {
		$.when(new getDatabaseStatsCommand(appUrl.getSystemDatabase()).execute(),
			new getStatusDebugConfigCommand(appUrl.getSystemDatabase()).execute()
			).done((stats: Array<databaseStatisticsDto>, config: any) => {
			this.allowVoron(stats[0].Is64Bit || config[0].Storage.Voron.AllowOn32Bits);
		});
	}

    deactivate() {
        // If we were closed via X button or other dialog dismissal, reject the deletion task since
        // we never started it.
        if (!this.creationTaskStarted) {
            this.creationTask.reject();
        }
    }

    isBundleActive(name: string): boolean {
        var licenseStatus: licenseStatusDto = this.licenseStatus();

        if (licenseStatus == null || licenseStatus.IsCommercial == false) {
            return true;
        }
        else {
            var value = licenseStatus.Attributes[name];
            return value === "true";
        }
    }

    nextOrCreate() {
        // For now we're just creating the filesystem.
        this.creationTaskStarted = true;
        dialog.close(this.parent);
        this.creationTask.resolve(this.fileSystemName(), this.getActiveBundles(), this.fileSystemPath(), this.fileSystemLogsPath(), this.storageEngine());
    }

    private isFilesystemNameExists(fileSystemName: string, filesystems: filesystem[]): boolean {
        fileSystemName = fileSystemName.toLowerCase();
        for (var i = 0; i < filesystems.length; i++) {
            if (fileSystemName == filesystems[i].name.toLowerCase()) {
                return true;
            }
        }
        return false;
    }

    private checkName(name: string): string {
        var rg1 = /^[^\\/:\*\?"<>\|]+$/; // forbidden characters \ / : * ? " < > |
        var rg2 = /^\./; // cannot start with dot (.)
        var rg3 = /^(nul|prn|con|lpt[0-9]|com[0-9])(\.|$)/i; // forbidden file names
        var maxLength = 260 - 30;

        var message = "";
        if (!$.trim(name)) {
            message = "Please fill out the file system name field!";
        }
        else if (name.length > maxLength) {
            message = "The file system length can't exceed " + maxLength + " characters!";
        }
        else if (!rg1.test(name)) {
            message = "The file system name can't contain any of the following characters: \ / : * ?" + ' " ' + "< > |";
        }
        else if (rg2.test(name)) {
            message = "The file system name can't start with a dot!";
        }
        else if (rg3.test(name)) {
            message = "The name '" + name + "' is forbidden for use!";
        }
        return message;       
    }

    private isPathLegal(name: string, pathName: string): string {
        var rg1 = /^[^*\?"<>\|]+$/; // forbidden characters \ * : ? " < > |
        var rg2 = /^(nul|prn|con|lpt[0-9]|com[0-9])(\.|$)/i; // forbidden file names
        var errorMessage = "";

        if (!$.trim(name) == false) { // if name isn't empty or not consist of only whitepaces
            if (name.length > 248) {
                errorMessage = "The path name for the '" + pathName + "' can't exceed " + 248 + " characters!";
            } else if (!rg1.test(name)) {
                errorMessage = "The " + pathName + " can't contain any of the following characters: * : ?" + ' " ' + "< > |";
            } else if (rg2.test(name)) {
                errorMessage = "The name '" + name + "' is forbidden for use!";
            }
        }
        return errorMessage;
    }

    toggleEncryptionBundle() {
        this.isEncryptionBundleEnabled.toggle();
    }

    toggleVersioningBundle() {
        this.isVersioningBundleEnabled.toggle();
    }

    private getActiveBundles(): string[] {
        var activeBundles: string[] = [];
        if (this.isEncryptionBundleEnabled()) {
            activeBundles.push("Encryption");
        }
        if (this.isVersioningBundleEnabled()) {
            activeBundles.push("Versioning");
        }
        return activeBundles;
    }
}

export = createFilesystem;
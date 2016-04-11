import dialog = require("plugins/dialog");
import appUrl = require("common/appUrl");
import filesystem = require("models/filesystem/filesystem");
import createResourceBase = require("viewmodels/resources/createResourceBase");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import getDatabaseStatsCommand = require("commands/resources/getDatabaseStatsCommand");
import getStatusDebugConfigCommand = require("commands/database/debug/getStatusDebugConfigCommand");
import shell = require("viewmodels/shell");

class createFilesystem extends createResourceBase {
    resourceNameCapitalString = "File System";
    resourceNameString = "file system";

    isEncryptionBundleEnabled = ko.observable(false);
    isVersioningBundleEnabled = ko.observable(false);

    constructor(parent: dialogViewModelBase) {
        super(shell.fileSystems, parent);
    }

    nextOrCreate() {
        this.creationTaskStarted = true;
        dialog.close(this.parent);
        this.creationTask.resolve(this.resourceName(), this.getActiveBundles(), this.resourcePath(), this.logsPath(), this.resourceTempPath(), this.storageEngine());
        this.clearResourceName();
    }

    private doesFileSystemNameExist(fileSystemName: string, fileSystems: filesystem[]): boolean {
        fileSystemName = fileSystemName.toLowerCase();
        for (var i = 0; i < fileSystems.length; i++) {
            if (fileSystemName === fileSystems[i].name.toLowerCase()) {
                return true;
            }
        }
        return false;
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

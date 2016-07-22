import viewModelBase = require("viewmodels/viewModelBase");
import filesystem = require('models/filesystem/filesystem');
import versioningEntry = require("models/filesystem/versioningEntry");
import appUrl = require("common/appUrl");
import getVersioningCommand = require("commands/filesystem/getVersioningCommand");
import saveVersioningCommand = require("commands/filesystem/saveVersioningCommand");

class versioning extends viewModelBase {
    versioning = ko.observable<versioningEntry>();
    isSaveEnabled: KnockoutComputed<boolean>;

    canActivate(args: any): any {
        super.canActivate(args);

        var deferred = $.Deferred();
        var fs = this.activeFilesystem();
        if (fs) {
            this.fetchVersioning(fs)
                .done(() => deferred.resolve({ can: true }))
                .fail(() => deferred.resolve({ redirect: appUrl.forFilesystemFiles(this.activeFilesystem()) }));
        }
        return deferred;
    }

    activate(args) {
        super.activate(args);
        this.updateHelpLink("RCLD9I");

        this.dirtyFlag = new ko.DirtyFlag([this.versioning]);
        this.isSaveEnabled = ko.computed<boolean>(() => this.dirtyFlag().isDirty());
    }

    private fetchVersioning(fs:filesystem): JQueryPromise<any> {
        var task: JQueryPromise<versioningEntry> = new getVersioningCommand(fs).execute();
        task.done((versioning: versioningEntry) => this.versioningLoaded(versioning));
        return task;
    }

    saveChanges() {
        var fs = this.activeFilesystem();
        if (fs) {
            var saveTask = new saveVersioningCommand(fs, this.versioning().toDto())
                .execute()
                .done((saveResult: bulkDocumentDto[]) => {
                    this.dirtyFlag().reset();
                });
        }
    }

    versioningLoaded(data: versioningEntry) {
        this.versioning(data);
        this.dirtyFlag().reset();
    }
}

export = versioning;

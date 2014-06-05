import app = require("durandal/app");
import system = require("durandal/system");
import router = require("plugins/router");
import appUrl = require("common/appUrl");
import shell = require("viewmodels/shell");

import viewModelBase = require("viewmodels/viewModelBase");
import resolveConflict = require("viewmodels/filesystem/resolveConflict");

import conflictItem = require("models/filesystem/conflictItem");
import filesystem = require("models/filesystem/filesystem");

import getFilesConflictsCommand = require("commands/filesystem/getFilesConflictsCommand");
import resolveConflictCommand = require("commands/filesystem/resolveConflictCommand");

class synchronizationConflicts extends viewModelBase {

    conflicts = ko.observableArray<conflictItem>();
    selectedConflicts = ko.observableArray<string>();
    isConflictsVisible = ko.computed(() => this.conflicts().length > 0);

    private isSelectAllValue = ko.observable<boolean>(false); 
    private activeFilesystemSubscription: any;
    private conflictsSubscription: any;

    activate(args) {
        super.activate(args);
        this.activeFilesystemSubscription = this.activeFilesystem.subscribe((fs: filesystem) => this.fileSystemChanged(fs));

        // treat notifications events
        this.conflictsSubscription = shell.currentFsChangesApi().watchFsConflicts((e: synchronizationConflictNotification) => {
            if (e.FileSystemName === this.activeFilesystem()) {
                switch (e.Type) {
                    case conflictType.ConflictDetected: {
                        this.addConflict(e);
                        break;
                    }
                    case conflictType.ConflictResolved: {
                        this.removeResolvedConflict(e);
                        break;
                    }
                    default:
                        console.error("unknown notification action");
                }
            }
        });

        this.loadConflicts();
    }

    deactivate() {
        super.deactivate();
        this.activeFilesystemSubscription.dispose();
        this.conflictsSubscription.off();
    }

    addConflict(conflictUpdate: synchronizationConflictNotification) {
        var match = this.conflictsContains(conflictUpdate);
        if (!match) {
            this.conflicts.push(conflictItem.fromConflictNotificationDto(conflictUpdate));
        }
    }

    removeResolvedConflict(conflictUpdate: synchronizationConflictNotification) {
        var match = this.conflictsContains(conflictUpdate);
        if (match) {
            this.conflicts.remove(match);
        }
    }

    private conflictsContains(e: synchronizationConflictNotification) : conflictItem {
        var match = ko.utils.arrayFirst(this.conflicts(), (item) => {
            return item.fileName === e.FileName;
        });

        return match;
    }


    loadConflicts(): JQueryPromise<any> {
        var fs = this.activeFilesystem();
        if (fs) {
            var deferred = $.Deferred();

            var conflictsTask = new getFilesConflictsCommand(fs).execute()
                .done(x => this.conflicts(x));

            conflictsTask.done(() => deferred.resolve());

            return deferred;
        }
    }

    collapseAll() {
        $(".synchronization-group-content").collapse('hide');
    }

    expandAll() {
        $(".synchronization-group-content").collapse('show');
    }
    
    resolveWithLocalVersion() {

        var message = this.selectedConflicts().length == 1 ?
            "Are you sure you want to resolve the conflict for file <b>" + this.selectedConflicts()[0] + "</b> by choosing the local version?" :
            "Are you sure you want to resolve the conflict for <b>" + this.selectedConflicts().length + "</b> selected files by choosing the local version?";

        require(["viewmodels/filesystem/resolveConflict"], resolveConflict => {
            var resolveConflictViewModel: resolveConflict = new resolveConflict(message, "Resolve conflict with local");
            resolveConflictViewModel
                .resolveTask
                .done(x => {
                    var fs = this.activeFilesystem();

                    for (var i = 0; i < this.selectedConflicts().length; i++) {
                        var conflict = this.selectedConflicts()[i];
                        new resolveConflictCommand(conflict, 1, fs).execute()
                            .done(this.modelPolling());
                    }
                });
            app.showDialog(resolveConflictViewModel);
        });
    }

    resolveWithRemoteVersion() {

        var message = this.selectedConflicts().length == 1 ?
            "Are you sure you want to resolve the conflict for file <b>" + this.selectedConflicts()[0] + "</b> by choosing the remote version?" :
            "Are you sure you want to resolve the conflict for <b>" + this.selectedConflicts().length + "</b> selected files by choosing the remote version?";

        require(["viewmodels/filesystem/resolveConflict"], resolveConflict => {
            var resolveConflictViewModel: resolveConflict = new resolveConflict(message, "Resolve conflict with remote");
            resolveConflictViewModel
                .resolveTask
                .done(x => {
                    var fs = this.activeFilesystem();

                    for (var i = 0; i < this.selectedConflicts().length; i++) {
                        var conflict = this.selectedConflicts()[i];
                        new resolveConflictCommand(conflict, 0, fs).execute()
                            .done(this.modelPolling());
                    }
                });
            app.showDialog(resolveConflictViewModel);
        });

    }

    fileSystemChanged(fs: filesystem) {
        if (fs) {
            this.loadConflicts();
        }
    }

    isSelectAll(): boolean {
        return this.isSelectAllValue();
    }

    toggleSelectAll() {
        this.isSelectAllValue(this.isSelectAllValue() ? false : true);
        if (this.isSelectAllValue()) {
            this.selectedConflicts.pushAll(this.conflicts().map(x => x.fileName));
        }
        else {
            this.selectedConflicts.removeAll();
        }
    }
}

export = synchronizationConflicts;
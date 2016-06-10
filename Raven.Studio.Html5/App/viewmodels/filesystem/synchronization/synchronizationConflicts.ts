import app = require("durandal/app");
import viewModelBase = require("viewmodels/viewModelBase");
import changesContext = require("common/changesContext");
import resolveConflict = require("viewmodels/filesystem/synchronization/resolveConflict");

import conflictItem = require("models/filesystem/conflictItem");
import filesystem = require("models/filesystem/filesystem");
import changeSubscription = require("common/changeSubscription");

import getFilesConflictsCommand = require("commands/filesystem/getFilesConflictsCommand");
import resolveConflictCommand = require("commands/filesystem/resolveConflictCommand");

class synchronizationConflicts extends viewModelBase {

    conflictStatus = {
        detected: "Detected",
        resolved: "Resolved"
    };

    conflicts = ko.observableArray<conflictItem>();
    selectedConflicts = ko.observableArray<string>();

    private isSelectAllValue = ko.observable<boolean>(false); 
    private activeFilesystemSubscription: any;

    activate(args) {
        super.activate(args);
        this.activeFilesystemSubscription = this.activeFilesystem.subscribe((fs: filesystem) => this.fileSystemChanged(fs));

        return this.loadConflicts();
    }

    deactivate() {
        super.deactivate();
        this.activeFilesystemSubscription.dispose();
    }

    createNotifications(): Array<changeSubscription> {
        return [changesContext.currentResourceChangesApi().watchFsConflicts((e: synchronizationConflictNotification) => this.processFsConflicts(e)) ];
    }

    private processFsConflicts(e: synchronizationConflictNotification) {
        switch (e.Status) {
            case this.conflictStatus.detected:
                this.addConflict(e);
                break;
            case this.conflictStatus.resolved:
                this.removeResolvedConflict(e);
                break;
        }
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
            this.selectedConflicts.remove(match.fileName);
        }
        this.isSelectAllValue(false);
    }

    private conflictsContains(e: synchronizationConflictNotification) : conflictItem {
        var match = ko.utils.arrayFirst(this.conflicts(), (item) => {
            return item.fileName === e.FileName;
        });

        return match;
    }

    private loadConflicts(): JQueryPromise<conflictItem[]> {
        var fs = this.activeFilesystem();
        return new getFilesConflictsCommand(fs)
            .execute()
            .done(x => this.conflicts(x));
    }

    resolveWithLocalVersion() {
        var message = this.selectedConflicts().length === 1 ?
            "Are you sure you want to resolve the conflict for file <b>" + this.selectedConflicts()[0] + "</b> by choosing the local version?" :
            "Are you sure you want to resolve the conflict for <b>" + this.selectedConflicts().length + "</b> selected files by choosing the local version?";

        var resolveConflictViewModel: resolveConflict = new resolveConflict(message, "Resolve conflict with local");
        resolveConflictViewModel
            .resolveTask
            .done(() => {
                var fs = this.activeFilesystem();

                var selectedConflicts = this.selectedConflicts();

                for (var i = 0; i < selectedConflicts.length; i++) {
                    var conflict = selectedConflicts[i];
                    new resolveConflictCommand(conflict, 2, fs).execute().done(() => {
                        this.selectedConflicts.remove(conflict);
                    });
                }
            });
        app.showDialog(resolveConflictViewModel);
    }

    resolveWithRemoteVersion() {
        var message = this.selectedConflicts().length === 1 ?
            "Are you sure you want to resolve the conflict for file <b>" + this.selectedConflicts()[0] + "</b> by choosing the remote version?" :
            "Are you sure you want to resolve the conflict for <b>" + this.selectedConflicts().length + "</b> selected files by choosing the remote version?";

        var resolveConflictViewModel: resolveConflict = new resolveConflict(message, "Resolve conflict with remote");
        resolveConflictViewModel
            .resolveTask
            .done(() => {
                var fs = this.activeFilesystem();

                for (var i = 0; i < this.selectedConflicts().length; i++) {
                    var conflict = this.selectedConflicts()[i];
                    new resolveConflictCommand(conflict, 1, fs).execute();
                }
            });
        app.showDialog(resolveConflictViewModel);
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
        this.isSelectAllValue(!this.isSelectAllValue());
        if (this.isSelectAllValue()) {
            this.selectedConflicts.pushAll(this.conflicts().map(x => x.fileName));
        }
        else {
            this.selectedConflicts.removeAll();
        }
    }
}

export = synchronizationConflicts;

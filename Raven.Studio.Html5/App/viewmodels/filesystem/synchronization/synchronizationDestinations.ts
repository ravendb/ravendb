import appUrl = require("common/appUrl");
import shell = require("viewmodels/shell");

import synchronizationReplicationSetup = require("models/filesystem/synchronizationReplicationSetup");
import synchronizationDestination = require("models/filesystem/synchronizationDestination");
import viewModelBase = require("viewmodels/viewModelBase");
import changesContext = require("common/changesContext");

import getDestinationsCommand = require("commands/filesystem/getDestinationsCommand");
import getFileSystemStatsCommand = require("commands/filesystem/getFileSystemStatsCommand");
import saveDestinationCommand = require("commands/filesystem/saveDestinationCommand");

class synchronizationDestinations extends viewModelBase {

    isSaveEnabled: KnockoutComputed<boolean>;
    dirtyFlag = new ko.DirtyFlag([]);
    replicationsSetup = ko.observable<synchronizationReplicationSetup>(new synchronizationReplicationSetup({ Destinations: [], Source: null }));
    subscription: any;
    saveIssued: boolean = false;

    canActivate(args: any): JQueryPromise<any> {
        super.canActivate(args);

        var deferred = $.Deferred();
        var fs = this.activeFilesystem();
        if (fs) {
            this.fetchDestinations()
                .done(() => deferred.resolve({ can: true }))
                .fail(() => deferred.resolve({ redirect: appUrl.forFilesystemFiles(this.activeFilesystem()) }));
        }
        return deferred;
    }

    activate(args) {
        super.activate(args);

        this.updateHelpLink("KW8LAF");
       
        if (!this.subscription) {
            this.subscription = changesContext.currentResourceChangesApi()
                .watchFsDestinations((e: filesystemConfigNotification) => {
                    if (e.Name.indexOf("Raven/Synchronization/Destinations") < 0)
                        return;
                    if (!this.saveIssued && (e.Action == filesystemConfigurationChangeAction.Set || e.Action == filesystemConfigurationChangeAction.Delete)) {
                        var canContinue = this.canContinueIfNotDirty('Data has changed', 'Data has changed in the server. Do you want to refresh and overwrite your changes?');
                        canContinue.done(() => {
                            this.fetchDestinations().done(x => {
                                this.dirtyFlag().reset();
                            });
                        }); 
                    }
                    else
                        console.error("Unknown notification action.");
                });
        }

        this.dirtyFlag = new ko.DirtyFlag([this.replicationsSetup]);
        this.isSaveEnabled = ko.computed(() => {
            return this.dirtyFlag().isDirty();
        });
    }

    deactivate() {
        super.deactivate();
    }

    saveChanges() {
        if (this.replicationsSetup().source()) {
            this.saveReplicationSetup();
        } else {
            var fs = this.activeFilesystem();
            if (fs) {
                new getFileSystemStatsCommand(fs)
                    .execute()
                    .done(result => this.prepareAndSaveReplicationSetup(result.DatabaseId));
            }
        }
    }

    private prepareAndSaveReplicationSetup(source: string) {
        this.replicationsSetup().source(source);
        this.saveReplicationSetup();
    }

    private saveReplicationSetup() {
        var fs = this.activeFilesystem();
        if (fs) {
            var self = this;
            this.saveIssued = true;
            new saveDestinationCommand(this.replicationsSetup().toDto(), fs)
                .execute()
                .done(() => {
                    console.log("Reseted dirty flag");
                    this.dirtyFlag().reset()
                }).always(() => this.saveIssued = false);;
        }
    }

    createNewDestination() {
        var fs = this.activeFilesystem();
        this.replicationsSetup().destinations.unshift(synchronizationDestination.empty(fs.name));
    }

    removeDestination(repl: synchronizationDestination) {
        this.replicationsSetup().destinations.remove(repl);
    }

    fetchDestinations(): JQueryPromise<any> {
        var deferred = $.Deferred();
        var fs = this.activeFilesystem();
        if (fs) {
            new getDestinationsCommand(fs)
                .execute()
                .done(data => this.replicationsSetup(new synchronizationReplicationSetup({ Destinations: data.Destinations, Source: null })))
                .always(() => deferred.resolve({ can: true }));
        }
        return deferred;
    }
}

export = synchronizationDestinations;

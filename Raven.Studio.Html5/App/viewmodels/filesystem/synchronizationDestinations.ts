import app = require("durandal/app");
import system = require("durandal/system");
import router = require("plugins/router");
import appUrl = require("common/appUrl");

import synchronizationReplicationSetup = require("models/filesystem/synchronizationReplicationSetup");
import synchronizationDestination = require("models/filesystem/synchronizationDestination");
import filesystem = require("models/filesystem/filesystem");

import viewModelBase = require("viewmodels/viewModelBase");

import getDestinationsCommand = require("commands/filesystem/getDestinationsCommand");
import getFileSystemStatsCommand = require("commands/filesystem/getFileSystemStatsCommand");
import saveDestinationCommand = require("commands/filesystem/saveDestinationCommand");


class synchronizationDestinations extends viewModelBase {

    isSaveEnabled: KnockoutComputed<boolean>;
    dirtyFlag = new ko.DirtyFlag([]);
    replicationsSetup = ko.observable<synchronizationReplicationSetup>(new synchronizationReplicationSetup({ Destinations: [], Source: null }));

    canActivate(args: any): JQueryPromise<any> {
        var deferred = $.Deferred();
        var fs = this.activeFilesystem();
        if (fs) {
            this.fetchDestinations()
                .done(() => deferred.resolve({ can: true }))
                .fail(() => deferred.resolve({ redirect: appUrl.forFilesystem(this.activeFilesystem()) }));
        }
        return deferred;
    }

    activate(args) {
        super.activate(args);
       
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
            new saveDestinationCommand(this.replicationsSetup().toDto(), fs)
                .execute()
                .done(() => this.dirtyFlag().reset());
        }
    }

    createNewDestination() {
        this.replicationsSetup().destinations.unshift(synchronizationDestination.empty());
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
import app = require("durandal/app");
import system = require("durandal/system");
import router = require("plugins/router");
import appUrl = require("common/appUrl");

import replicationsSetup = require("models/replicationsSetup");
import synchronizationDestination = require("models/filesystem/synchronizationDestination");
import filesystem = require("models/filesystem/filesystem");

import viewModelBase = require("viewmodels/viewModelBase");

import getDestinationsCommand = require("commands/filesystem/getDestinationsCommand");


class synchronizationDestinations extends viewModelBase {

    destinations = ko.observableArray<synchronizationDestination>();
    isSaveEnabled: KnockoutComputed<boolean>;
    private activeFilesystemSubscription: any;

    activate(args) {
        super.activate(args);

        this.isSaveEnabled = ko.computed(() => true);
        this.activeFilesystemSubscription = this.activeFilesystem.subscribe((fs: filesystem) => this.fileSystemChanged(fs));
    }

    deactivate() {
        super.deactivate();
        this.activeFilesystemSubscription.dispose();
    }

    modelPolling() {
        this.loadDestinations();
    }

    saveChanges() {
        
    }

    createNewDestination() {
        this.destinations.unshift(synchronizationDestination.empty());
    }

    removeDestination(repl: synchronizationDestination) {
        this.destinations.remove(repl);
    }

    loadDestinations(): JQueryPromise<any> {
        var fs = this.activeFilesystem();
        if (fs) {
            return new getDestinationsCommand(fs).execute()
                       .done(data => this.destinations(data));
        }
    }

    fileSystemChanged(fs: filesystem) {
        if (fs) {
            this.modelPollingStop();
            this.loadDestinations().always(() => {
                this.modelPollingStart();
            });
        }
    }
}

export = synchronizationDestinations;
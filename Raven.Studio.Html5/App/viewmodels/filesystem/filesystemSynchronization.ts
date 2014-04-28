import app = require("durandal/app");
import system = require("durandal/system");
import router = require("plugins/router");
import appUrl = require("common/appUrl");

import viewModelBase = require("viewmodels/viewModelBase");

import synchronizationDetails = require("models/filesystem/synchronizationDetails");
import synchronizationDestination = require("models/filesystem/synchronizationDestination");

import getDestinationsCommand = require("commands/filesystem/getDestinationsCommand");
import getFilesConflictsCommand = require("commands/filesystem/getFilesConflictsCommand");
import getSyncOutgoingActivitiesCommand = require("commands/filesystem/getSyncOutgoingActivitiesCommand");
import getSyncIncomingActivitiesCommand = require("commands/filesystem/getSyncIncomingActivitiesCommand");
import saveDestinationCommand = require("commands/filesystem/saveDestinationCommand");
import deleteDestinationCommand = require("commands/filesystem/deleteDestinationCommand");
import synchronizeNowCommand = require("commands/filesystem/synchronizeNowCommand");
import synchronizeWithDestinationCommand = require("commands/filesystem/synchronizeWithDestinationCommand");

import filesystemAddDestination = require("viewmodels/filesystem/filesystemAddDestination");

class filesystemSynchronization extends viewModelBase {

    destinations = ko.observableArray<string>();
    isDestinationsVisible = ko.computed(() => this.destinations().length > 0); 

    conflicts = ko.observableArray<string>();      
    isConflictsVisible = ko.computed(() => this.conflicts().length > 0); 

    outgoingActivity = ko.observableArray<synchronizationDetails>();   
    isOutgoingActivityVisible = ko.computed(() => this.outgoingActivity().length > 0); 
       
    incomingActivity = ko.observableArray<synchronizationDetails>();      
    isIncomingActivityVisible = ko.computed(() => this.incomingActivity().length > 0);     
          
    private router = router;
    synchronizationUrl = appUrl.forCurrentDatabase().filesystemSynchronization;

    constructor() {
        super();        
    }

    canActivate(args: any) {
        return true;
    }

    activate(args) {
        super.activate(args);
    }

    addDestination() {
        var fs = this.activeFilesystem();
        require(["viewmodels/filesystem/filesystemAddDestination"], filesystemAddDestination => {
            var addDestinationViewModel: filesystemAddDestination = new filesystemAddDestination(this.destinations);
            addDestinationViewModel
                .creationTask
                .done((destinationUrl: string) => this.addDestinationUrl(new synchronizationDestination(fs, destinationUrl)));
            app.showDialog(addDestinationViewModel);
        });
    }

    private addDestinationUrl(url: synchronizationDestination) {
        var fs = this.activeFilesystem();
        if (fs) {        
            var self = this;    
            new saveDestinationCommand(fs, url).execute()
                .done(x => self.forceModelPolling());
        }
    }

    synchronizeNow() {
        var fs = this.activeFilesystem();
        if (fs) {
            new synchronizeNowCommand(fs).execute();
        }
    }

    synchronizeWithDestination(destination: string) {
        var fs = this.activeFilesystem();
        if (fs) {
            new synchronizeWithDestinationCommand(fs, destination).execute();
        }
    }

    deleteDestination(destination: string) {
        var fs = this.activeFilesystem();
        var self = this;
        if (fs) {
            new deleteDestinationCommand(fs, destination).execute()
                .done(x => self.forceModelPolling());
        }
    }

    modelPolling() {

        var fs = this.activeFilesystem();
        if (fs) {
            new getDestinationsCommand(fs).execute()
                .done(data => this.destinations(data));        

            new getFilesConflictsCommand(fs).execute()
                .done(x => this.conflicts(x));                                    

            new getSyncOutgoingActivitiesCommand(fs).execute()
                .done(x => this.outgoingActivity(x));

            new getSyncIncomingActivitiesCommand(fs).execute()
                .done(x => this.incomingActivity(x));
        }
    }


    collapseAll() {
        $(".synchronization-group-content").collapse('hide');
    }

    expandAll() {
        $(".synchronization-group-content").collapse('show');
    }
}

export = filesystemSynchronization;
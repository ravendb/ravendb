import app = require("durandal/app");
import system = require("durandal/system");
import router = require("plugins/router");
import appUrl = require("common/appUrl");

import viewModelBase = require("viewmodels/viewModelBase");

import synchronizationDetails = require("models/filesystem/synchronizationDetails");

import getDestinationsCommand = require("commands/filesystem/getDestinationsCommand");
import getFilesConflictsCommand = require("commands/filesystem/getFilesConflictsCommand");
import getSyncOutgoingActivitiesCommand = require("commands/filesystem/getSyncOutgoingActivitiesCommand");
import getSyncIncomingActivitiesCommand = require("commands/filesystem/getSyncIncomingActivitiesCommand");
import saveFilesystemDestinationCommand = require("commands/filesystem/saveDestinationCommand");
import filesystemAddDestination = require("viewmodels/filesystem/filesystemAddDestination");

class filesystemSynchronization extends viewModelBase {

    destinations = ko.observableArray<string>();      
    conflicts = ko.observable<string>();      
    outgoingActivity = ko.observable<synchronizationDetails>();      
    incomingActivity = ko.observable<synchronizationDetails>();          
          
    private router = router;
    synchronizationUrl = appUrl.forCurrentDatabase().filesystemSynchronization;

    constructor() {
        super();
    }

    activate(args) {
        super.activate(args);
    }

    addDestination() {
        require(["viewmodels/filesystem/filesystemAddDestination"], filesystemAddDestination => {
            var addDestinationViewModel: filesystemAddDestination = new filesystemAddDestination(this.destinations);
            addDestinationViewModel
                .creationTask
                .done((destinationUrl: string) => this.addDestinationUrl(destinationUrl));
            app.showDialog(addDestinationViewModel);
        });
    }

    private addDestinationUrl(url: string) {
        var fs = this.activeFilesystem();
        if (fs) {            
            new saveFilesystemDestinationCommand(fs, url).execute(); 
        }
    }

    synchronizeNow() {

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
}

export = filesystemSynchronization;
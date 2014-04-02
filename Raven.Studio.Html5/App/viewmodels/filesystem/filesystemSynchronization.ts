import app = require("durandal/app");
import system = require("durandal/system");
import router = require("plugins/router");
import appUrl = require("common/appUrl");

import viewModelBase = require("viewmodels/viewModelBase");

import getConfigurationByKeyCommand = require("commands/filesystem/getConfigurationByKeyCommand");
import saveFilesystemDestinationCommand = require("commands/filesystem/saveFilesystemDestinationCommand");


import filesystemAddDestination = require("viewmodels/filesystem/filesystemAddDestination");

class filesystemSynchronization extends viewModelBase {

    destinations = ko.observableArray<string>();    
    private router = router;
    synchronizationUrl = appUrl.forCurrentDatabase().filesystemSynchronization;

    constructor() {
        super();
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
        if (fs)
        {            
            new saveFilesystemDestinationCommand(fs, url).execute(); 
        }
    }
}

export = filesystemSynchronization;
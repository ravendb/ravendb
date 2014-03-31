import app = require("durandal/app");
import system = require("durandal/system");
import router = require("plugins/router");
import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");

import addDestination = require("viewmodels/filesystemAddDestination");

class filesystemSynchronization extends viewModelBase {

    destinations = ko.observableArray<string>();    
    private router = router;
    synchronizationUrl = appUrl.forCurrentDatabase().filesystemSynchronization;

    constructor() {
        super();
    }

    addDestination() {
        require(["viewmodels/filesystemAddDestination"], addDestination => {
            var addDestinationViewModel: addDestination = new addDestination(this.destinations);
            addDestinationViewModel
                .creationTask
                .done();
            app.showDialog(addDestinationViewModel);
        });
    }

}

export = filesystemSynchronization;
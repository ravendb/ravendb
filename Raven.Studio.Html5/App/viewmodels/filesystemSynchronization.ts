import app = require("durandal/app");
import system = require("durandal/system");
import router = require("plugins/router");
import appUrl = require("common/appUrl");
import filesystem = require("models/filesystem");
import viewModelBase = require("viewmodels/viewModelBase");

class filesystemSynchronization extends viewModelBase {

    private router = router;

    synchronizationUrl = appUrl.forCurrentDatabase().filesystemSynchronization;

    constructor() {
        super();
    }

}

export = filesystemSynchronization;
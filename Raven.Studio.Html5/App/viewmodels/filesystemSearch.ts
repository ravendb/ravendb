import app = require("durandal/app");
import system = require("durandal/system");
import router = require("plugins/router");
import appUrl = require("common/appUrl");
import filesystem = require("models/filesystem");
import viewModelBase = require("viewmodels/viewModelBase");

class filesystemSearch extends viewModelBase {

    private router = router;

    searchUrl = appUrl.forCurrentDatabase().filesystemSearch;

    constructor() {
        super();

    }

}

export = filesystemSearch;
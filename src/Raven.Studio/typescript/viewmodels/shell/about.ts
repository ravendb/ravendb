import viewModelBase = require("viewmodels/viewModelBase");
import shell = require("viewmodels/shell");

class about extends viewModelBase {

    clientVersion = shell.clientVersion;
    serverVersion = shell.serverBuildVersion;

}

export = about;
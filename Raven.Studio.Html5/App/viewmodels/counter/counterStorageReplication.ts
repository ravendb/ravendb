import viewModelBase = require("viewmodels/viewModelBase");

class counterStorageReplication extends viewModelBase {
    canActivate(args: any): any {
        return true;
    }
}

export = counterStorageReplication;
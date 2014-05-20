import viewModelBase = require("viewmodels/viewModelBase");

class counterStoragecounters extends viewModelBase {
    canActivate(args: any): any {
        return true;
    }
}

export = counterStoragecounters;
import viewModelBase = require("viewmodels/viewModelBase");

class counterStorageStats extends viewModelBase {
    canActivate(args: any): any {
        return true;
    }
}

export = counterStorageStats;

import viewModelBase = require("viewmodels/viewModelBase");

class counterStorageConfiguration extends viewModelBase {
    canActivate(args: any): any {
        return true;
    }
}

export = counterStorageConfiguration;

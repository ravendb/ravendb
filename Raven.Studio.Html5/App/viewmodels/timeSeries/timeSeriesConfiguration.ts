import viewModelBase = require("viewmodels/viewModelBase");

class timeSeriesConfiguration extends viewModelBase {
    canActivate(args: any): any {
        return true;
    }
}

export = timeSeriesConfiguration;
import viewModelBase = require("viewmodels/viewModelBase");

class timeSeriesStats extends viewModelBase {
    canActivate(args: any): any {
        return true;
    }
}

export = timeSeriesStats;
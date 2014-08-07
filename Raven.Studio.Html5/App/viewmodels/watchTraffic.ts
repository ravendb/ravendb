import viewModelBase = require("viewmodels/viewModelBase");
import watchTrafficConfigDialog = require("viewmodels/watchTrafficConfigDialog");
class watchTraffic extends viewModelBase {
    logConfig = ko.observable<{ ResourceName: string; MaxEntries: number; WatchedResourceMode: string; SingleAuthToken: singleAuthToken}>();


    constructor() {
        super();
    }


    canActivate(args) {
            
    }


    configureConnection() {
        
    }


}

export =watchTraffic;
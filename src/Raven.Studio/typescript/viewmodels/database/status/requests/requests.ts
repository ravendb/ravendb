import viewModelBase = require("viewmodels/viewModelBase");
import appUrl = require("common/appUrl");
import status = require("viewmodels/database/status/status");
import accessHelper = require("viewmodels/shell/accessHelper");

class requests extends viewModelBase {
    router: DurandalRouter;
    currentRouteTitle: KnockoutComputed<string>;

    constructor() {
        super();

        this.router = status.statusRouter.createChildRouter()
            .map([
                { route: 'databases/status/requests', moduleId: 'viewmodels/database/status/requests/requestsCount', title: 'Requests count', tooltip: "Displays requests counts over time", nav: true, hash: appUrl.forCurrentDatabase().requestsCount },
                { route: 'databases/status/requests/tracing', moduleId: 'viewmodels/database/status/requests/requestTracing', title: 'Request tracing', tooltip: "Displays recent requests with their status and execution times", nav: accessHelper.canExposeConfigOverTheWire(), hash: appUrl.forCurrentDatabase().requestsTracing },
            ])
            .buildNavigationModel();
       
        appUrl.mapUnknownRoutes(this.router);

        this.currentRouteTitle = ko.computed(() => {
            // Is there a better way to get the active route?
            var activeRoute = this.router.navigationModel().first(r => r.isActive());
            return activeRoute != null ? activeRoute.title : "";
        });
    }
}

export = requests;    

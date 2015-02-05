import durandalRouter = require("plugins/router");
import database = require("models/database");
import viewModelBase = require("viewmodels/viewModelBase");
import appUrl = require("common/appUrl");
import status = require("viewmodels/status");
import shell = require('viewmodels/shell');

class requests extends viewModelBase {

    router: DurandalRouter;
    currentRouteTitle: KnockoutComputed<string>;

    constructor() {
        super();

        this.router = status.statusRouter.createChildRouter()
            .map([
                { route: 'databases/status/requests', moduleId: 'viewmodels/requestsCount', title: 'Requests count', tooltip: "Displays requests counts over time", nav: true, hash: appUrl.forCurrentDatabase().requestsCount },
                { route: 'databases/status/requests/tracing', moduleId: 'viewmodels/requestTracing', title: 'Request tracing', tooltip: "Displays recent requests with their status and execution times", nav: shell.canExposeConfigOverTheWire(), hash: appUrl.forCurrentDatabase().requestsTracing },
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
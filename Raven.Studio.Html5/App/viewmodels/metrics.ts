import durandalRouter = require("plugins/router");
import database = require("models/database");
import viewModelBase = require("viewmodels/viewModelBase");
import appUrl = require("common/appUrl");

import status = require("viewmodels/status");

class metrics extends viewModelBase {

    router: DurandalRouter;
    currentRouteTitle: KnockoutComputed<string>;

    constructor() {
        super();

        this.router = status.statusRouter.createChildRouter()
            .map([
                { route: 'databases/status/metrics',                  moduleId: 'viewmodels/metricsIndexing',              title: 'Indexing performance',  tooltip: "TODO", nav: true, hash: appUrl.forCurrentDatabase().metrics },
                { route: 'databases/status/metrics/requests',         moduleId: 'viewmodels/metricsRequests',              title: 'Requests count',        tooltip: "TODO", nav: true, hash: appUrl.forCurrentDatabase().metricsRequests },
                { route: 'databases/status/metrics/indexBatchSize',   moduleId: 'viewmodels/metricsIndexBatchSize',        title: 'Index batch size',      tooltip: "TODO", nav: true, hash: appUrl.forCurrentDatabase().metricsIndexBatchSize },
                { route: 'databases/status/metrics/prefetches',       moduleId: 'viewmodels/metricsPrefetches',            title: 'Prefetches',            tooltip: "TODO", nav: true, hash: appUrl.forCurrentDatabase().metricsPrefetches },
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

export = metrics;    
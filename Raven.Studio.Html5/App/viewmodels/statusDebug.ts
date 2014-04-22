import durandalRouter = require("plugins/router");
import database = require("models/database");
import viewModelBase = require("viewmodels/viewModelBase");
import appUrl = require("common/appUrl");

import status = require("viewmodels/status");

class statusDebug extends viewModelBase {

    router: DurandalRouter;
    currentRouteTitle: KnockoutComputed<string>;
    appUrls: computedAppUrls;

	constructor() {
        super();
        
        this.appUrls = appUrl.forCurrentDatabase();

        this.router = status.statusRouter.createChildRouter()
            .map([
                { route: 'status/debug',                    moduleId: 'viewmodels/statusDebugChanges',           title: 'Changes',            nav: true, hash: appUrl.forCurrentDatabase().statusDebugChanges },
                { route: 'status/debug/metrics',            moduleId: 'viewmodels/statusDebugMetrics',           title: 'Metrics',            nav: true, hash: appUrl.forCurrentDatabase().statusDebugMetrics },
                { route: 'status/debug/config',             moduleId: 'viewmodels/statusDebugConfig',            title: 'Config',             nav: true, hash: appUrl.forCurrentDatabase().statusDebugConfig },
                { route: 'status/debug/docrefs',            moduleId: 'viewmodels/statusDebugDocrefs',           title: 'Doc refs',           nav: true, hash: appUrl.forCurrentDatabase().statusDebugDocrefs },
                { route: 'status/debug/currentlyIndexing',  moduleId: 'viewmodels/statusDebugCurrentlyIndexing', title: 'Currently indexing', nav: true, hash: appUrl.forCurrentDatabase().statusDebugCurrentlyIndexing },
                { route: 'status/debug/queries',            moduleId: 'viewmodels/statusDebugQueries',           title: 'Queries',            nav: true, hash: appUrl.forCurrentDatabase().statusDebugQueries },
                { route: 'status/debug/tasks',              moduleId: 'viewmodels/statusDebugTasks',             title: 'Tasks',              nav: true, hash: appUrl.forCurrentDatabase().statusDebugTasks },
            ])
            .buildNavigationModel();


        this.currentRouteTitle = ko.computed(() => {
            // Is there a better way to get the active route?
            var activeRoute = this.router.navigationModel().first(r => r.isActive());
            return activeRoute != null ? activeRoute.title : "";
        });
    }
}

export = statusDebug;    
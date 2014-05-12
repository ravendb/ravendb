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
                { route: 'databases/status/debug',                   moduleId: 'viewmodels/statusDebugChanges',           title: 'Changes',            nav: true, hash: appUrl.forCurrentDatabase().statusDebugChanges },
                { route: 'databases/status/debug/metrics',           moduleId: 'viewmodels/statusDebugMetrics',           title: 'Metrics',            nav: true, hash: appUrl.forCurrentDatabase().statusDebugMetrics },
                { route: 'databases/status/debug/config',            moduleId: 'viewmodels/statusDebugConfig',            title: 'Config',             nav: true, hash: appUrl.forCurrentDatabase().statusDebugConfig },
                { route: 'databases/status/debug/docrefs',           moduleId: 'viewmodels/statusDebugDocrefs',           title: 'Doc refs',           nav: true, hash: appUrl.forCurrentDatabase().statusDebugDocrefs },
                { route: 'databases/status/debug/currentlyIndexing', moduleId: 'viewmodels/statusDebugCurrentlyIndexing', title: 'Currently indexing', nav: true, hash: appUrl.forCurrentDatabase().statusDebugCurrentlyIndexing },
                { route: 'databases/status/debug/queries',           moduleId: 'viewmodels/statusDebugQueries',           title: 'Queries',            nav: true, hash: appUrl.forCurrentDatabase().statusDebugQueries },
                { route: 'databases/status/debug/tasks',             moduleId: 'viewmodels/statusDebugTasks',             title: 'Tasks',              nav: true, hash: appUrl.forCurrentDatabase().statusDebugTasks },
                { route: 'databases/status/debug/routes',            moduleId: 'viewmodels/statusDebugRoutes',            title: 'Routes',             nav: true, hash: appUrl.forCurrentDatabase().statusDebugRoutes },
                { route: 'databases/status/debug/requestTracing',    moduleId: 'viewmodels/statusDebugRequestTracing',    title: 'Request tracing',    nav: true, hash: appUrl.forCurrentDatabase().statusDebugRequestTracing },
                { route: 'databases/status/debug/sqlReplication',    moduleId: 'viewmodels/statusDebugSqlReplication',    title: 'Sql Replication',    nav: true, hash: appUrl.forCurrentDatabase().statusDebugSqlReplication },
                { route: 'databases/status/debug/indexFields',       moduleId: 'viewmodels/statusDebugIndexFields',       title: 'Index fields',       nav: true, hash: appUrl.forCurrentDatabase().statusDebugIndexFields },
                { route: 'databases/status/debug/slowDocCounts',     moduleId: 'viewmodels/statusDebugSlowDocCounts',     title: 'Slow doc counts',    nav: true, hash: appUrl.forCurrentDatabase().statusDebugSlowDocCounts },
                { route: 'databases/status/debug/identities',        moduleId: 'viewmodels/statusDebugIdentities',        title: 'Identities',         nav: true, hash: appUrl.forCurrentDatabase().statusDebugIdentities },
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
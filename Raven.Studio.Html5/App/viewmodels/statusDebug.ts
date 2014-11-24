import durandalRouter = require("plugins/router");
import database = require("models/database");
import viewModelBase = require("viewmodels/viewModelBase");
import appUrl = require("common/appUrl");
import status = require("viewmodels/status");

class statusDebug extends viewModelBase {

    router: DurandalRouter;
    currentRouteTitle: KnockoutComputed<string>;

	constructor() {
        super();

        this.router = status.statusRouter.createChildRouter()
            .map([
                { route: 'databases/status/debug',                   moduleId: 'viewmodels/statusDebugChanges',           title: 'Changes',           tooltip: "Shows information about active changes API subscriptions", nav: true, hash: appUrl.forCurrentDatabase().statusDebugChanges },
                { route: 'databases/status/debug/metrics',           moduleId: 'viewmodels/statusDebugMetrics',           title: 'Metrics',           tooltip: "Shows database metrics", nav: true, hash: appUrl.forCurrentDatabase().statusDebugMetrics },
                { route: 'databases/status/debug/config',            moduleId: 'viewmodels/statusDebugConfig',            title: 'Config',            tooltip: "Displays server configuration", nav: true, hash: appUrl.forCurrentDatabase().statusDebugConfig },
                { route: 'databases/status/debug/docrefs',           moduleId: 'viewmodels/statusDebugDocrefs',           title: 'Doc refs',          tooltip: "Allows to find documents referenced by given document id", nav: true, hash: appUrl.forCurrentDatabase().statusDebugDocrefs },
                { route: 'databases/status/debug/currentlyIndexing', moduleId: 'viewmodels/statusDebugCurrentlyIndexing', title: 'Currently indexing',tooltip: "Displays currently performed indexing work", nav: true, hash: appUrl.forCurrentDatabase().statusDebugCurrentlyIndexing },
                { route: 'databases/status/debug/queries',           moduleId: 'viewmodels/statusDebugQueries',           title: 'Queries',           tooltip: "Displays currently running queries", nav: true, hash: appUrl.forCurrentDatabase().statusDebugQueries },
                { route: 'databases/status/debug/tasks',             moduleId: 'viewmodels/statusDebugTasks',             title: 'Tasks',             tooltip: "Displays currently running index tasks", nav: true, hash: appUrl.forCurrentDatabase().statusDebugTasks },
                { route: 'databases/status/debug/routes',            moduleId: 'viewmodels/statusDebugRoutes',            title: 'Routes',            tooltip: "Displays all available routes", nav: true, hash: appUrl.forCurrentDatabase().statusDebugRoutes },
                { route: 'databases/status/debug/requestTracing',    moduleId: 'viewmodels/statusDebugRequestTracing',    title: 'Request tracing',   tooltip: "Displays recent requests with their statuses and execution times", nav: true, hash: appUrl.forCurrentDatabase().statusDebugRequestTracing },
                { route: 'databases/status/debug/sqlReplication',    moduleId: 'viewmodels/statusDebugSqlReplication',    title: 'Sql Replication',   tooltip: "Shows information about sql replication", nav: true, hash: appUrl.forCurrentDatabase().statusDebugSqlReplication },
                { route: 'databases/status/debug/indexFields',       moduleId: 'viewmodels/statusDebugIndexFields',       title: 'Index fields',      tooltip: "Shows names of indexed fields based on entered index definition", nav: true, hash: appUrl.forCurrentDatabase().statusDebugIndexFields },
                { route: 'databases/status/debug/slowDocCounts',     moduleId: 'viewmodels/statusDebugSlowDocCounts',     title: 'Doc counts',        tooltip: "Shows document counts (VERY SLOW)", nav: true, hash: appUrl.forCurrentDatabase().statusDebugSlowDocCounts },
                { route: 'databases/status/debug/identities',        moduleId: 'viewmodels/statusDebugIdentities',        title: 'Identities',        tooltip: "Shows identities values for collections", nav: true, hash: appUrl.forCurrentDatabase().statusDebugIdentities },
                { route: 'databases/status/debug/websocket',         moduleId: 'viewmodels/statusDebugWebSocket',         title: 'Web Socket',        tooltip: "Allows to debug websockets connection", nav: true, hash: appUrl.forCurrentDatabase().statusDebugWebSocket },
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

export = statusDebug;    
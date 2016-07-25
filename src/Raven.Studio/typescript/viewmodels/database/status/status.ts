import durandalRouter = require("plugins/router");
import database = require("models/resources/database");
import viewModelBase = require("viewmodels/viewModelBase");
import appUrl = require("common/appUrl");
import shell = require('viewmodels/shell');

class status extends viewModelBase {

    static initialVisibleViews = ["Stats", "Indexing", 'Request Tracking', 'Logs', 'Running Tasks', 'Alerts', 'Index Errors', 'User Info', 'Map/Reduce Visualizer', 'Debug', 'Storage', 'Gather Debug Info'];

    router: DurandalRootRouter;
    static statusRouter: DurandalRouter; //TODO: is it better way of exposing this router to child router?
    currentRouteTitle: KnockoutComputed<string>;
    appUrls: computedAppUrls;

    private bundleMap: dictionary<string> = { replication: "Replication Stats", sqlreplication: "SQL Replication Stats" };
    userDatabasePages = ko.observableArray(status.initialVisibleViews.slice());

    constructor() {
        super();
            
        this.appUrls = appUrl.forCurrentDatabase();

        this.router = durandalRouter.createChildRouter()
            .map([
                { route: 'databases/status', moduleId: 'viewmodels/database/status/statistics', title: 'Stats', nav: true, hash: appUrl.forCurrentDatabase().status },
                { route: 'databases/status/indexing*details', moduleId: 'viewmodels/database/status/indexing/indexing', title: 'Indexing', nav: true, hash: appUrl.forCurrentDatabase().indexPerformance },
                { route: 'databases/status/requests*details', moduleId: 'viewmodels/database/status/requests/requests', title: 'Request Tracking', nav: true, hash: appUrl.forCurrentDatabase().requestsCount },
                { route: 'databases/status/logs', moduleId: 'viewmodels/database/status/logs', title: 'Logs', nav: true, hash: appUrl.forCurrentDatabase().logs },
                { route: 'databases/status/runningTasks', moduleId: 'viewmodels/database/status/runningTasks', title: 'Running Tasks', nav: true, hash: appUrl.forCurrentDatabase().runningTasks },
                { route: 'databases/status/alerts', moduleId: 'viewmodels/database/status/alerts', title: 'Alerts', nav: true, hash: appUrl.forCurrentDatabase().alerts },
                { route: 'databases/status/indexErrors', moduleId: 'viewmodels/database/status/indexErrors', title: 'Index Errors', nav: true, hash: appUrl.forCurrentDatabase().indexErrors },
                { route: 'databases/status/replicationStats', moduleId: 'viewmodels/database/status/replicationStats', title: 'Replication Stats', nav: true, hash: appUrl.forCurrentDatabase().replicationStats },
                { route: 'databases/status/sqlReplicationPerfStats', moduleId: 'viewmodels/database/status/sqlReplicationPerfStats', title: 'SQL Replication Stats', nav: true, hash: appUrl.forCurrentDatabase().sqlReplicationPerfStats },
                { route: 'databases/status/userInfo', moduleId: 'viewmodels/database/status/userInfo', title: 'User Info', nav: true, hash: appUrl.forCurrentDatabase().userInfo },
                { route: 'databases/status/visualizer', moduleId: 'viewmodels/database/status/visualizer', title: 'Map/Reduce Visualizer', nav: true, hash: appUrl.forCurrentDatabase().visualizer },
                { route: 'databases/status/debug*details', moduleId: 'viewmodels/database/status/debug/statusDebug', title: 'Debug', nav: true, hash: appUrl.forCurrentDatabase().statusDebug },
                { route: 'databases/status/storage*details', moduleId: 'viewmodels/database/status/storage/statusStorage', title: 'Storage', nav: true, hash: appUrl.forCurrentDatabase().statusStorageOnDisk },
                { route: 'databases/status/infoPackage', moduleId: 'viewmodels/manage/infoPackage', title: 'Gather Debug Info', nav: shell.canExposeConfigOverTheWire(), hash: appUrl.forCurrentDatabase().infoPackage }
            ])
            .buildNavigationModel();

        status.statusRouter = this.router;

        this.router.guardRoute = (instance: Object, instruction: DurandalRouteInstruction) => this.getValidRoute(instance, instruction);

        appUrl.mapUnknownRoutes(this.router);

        this.currentRouteTitle = ko.computed(() => {
            // Is there a better way to get the active route?
            var activeRoute = this.router.navigationModel().first(r => r.isActive());
            return activeRoute != null ? activeRoute.title : "";
        });
    }

    /**
    * Checks whether the route can be navigated to. Returns true if it can be navigated to, or a redirect URI if it can't be navigated to.
    * This is used for preventing a navigating to system-only pages when the current databagse is non-system, and vice-versa.
    */
    getValidRoute(instance: Object, instruction: DurandalRouteInstruction): any {
        var db: database = this.activeDatabase();
        var pathArr = instruction.fragment.split('/');
        var bundelName = pathArr[pathArr.length - 1].toLowerCase();
        var isLegalBundelName = (this.bundleMap[bundelName] != undefined);
        var isBundleExists = this.userDatabasePages.indexOf(this.bundleMap[bundelName]) > -1;

        if (isLegalBundelName && isBundleExists == false) {
            return appUrl.forCurrentDatabase().databaseSettings();
        }

        return true;
    }

    activate(args: any) {
        super.activate(args);

        this.userDatabasePages(status.initialVisibleViews.slice());
        var db: database = this.activeDatabase();
        var bundles: string[] = db.activeBundles();

        bundles.forEach((bundle: string) => {
            var bundleName = this.bundleMap[bundle.toLowerCase()];
            if (bundleName != undefined) {
                this.userDatabasePages.push(bundleName);
            }
        });
    }

    routeIsVisible(route: DurandalRouteConfiguration) {
        var bundleTitle = route.title;

        if (this.userDatabasePages.indexOf(bundleTitle) !== -1) {
            // Replication stats and Sql Replication stats are visible only when we're on a user database.
            return true;
        }

        return false;
    }
}

export = status;    

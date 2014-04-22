import durandalRouter = require("plugins/router");
import database = require("models/database");
import viewModelBase = require("viewmodels/viewModelBase");
import appUrl = require("common/appUrl");

class status extends viewModelBase {

    router: DurandalRootRouter;
    static statusRouter: DurandalRouter; //TODO: is it better way of exposing this router to child router?
    currentRouteTitle: KnockoutComputed<string>;
    appUrls: computedAppUrls;

	constructor() {
        super();
        
        this.appUrls = appUrl.forCurrentDatabase();

        this.router = durandalRouter.createChildRouter()
            .map([
                { route: 'status',                  moduleId: 'viewmodels/statistics',          title: 'Stats',             nav: true, hash: appUrl.forCurrentDatabase().status },
                { route: 'status/logs',             moduleId: 'viewmodels/logs',                title: 'Logs',              nav: true, hash: appUrl.forCurrentDatabase().logs },
                { route: 'status/alerts',           moduleId: 'viewmodels/alerts',              title: 'Alerts',            nav: true, hash: appUrl.forCurrentDatabase().alerts },
                { route: 'status/indexErrors',      moduleId: 'viewmodels/indexErrors',         title: 'Index Errors',      nav: true, hash: appUrl.forCurrentDatabase().indexErrors },
                { route: 'status/replicationStats', moduleId: 'viewmodels/replicationStats',    title: 'Replication Stats', nav: true, hash: appUrl.forCurrentDatabase().replicationStats },
                { route: 'status/userInfo',         moduleId: 'viewmodels/userInfo',            title: 'User Info',         nav: true, hash: appUrl.forCurrentDatabase().userInfo },
                { route: 'status/debug*details',    moduleId: 'viewmodels/statusDebug',         title: 'Debug',             nav: true, hash: appUrl.forCurrentDatabase().statusDebug },
			])
            .buildNavigationModel();

        status.statusRouter = this.router;


        this.currentRouteTitle = ko.computed(() => {
            // Is there a better way to get the active route?
            var activeRoute = this.router.navigationModel().first(r => r.isActive());
            return activeRoute != null ? activeRoute.title : "";
        });
    }
}

export = status;    
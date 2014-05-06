import durandalRouter = require("plugins/router");
import database = require("models/database");
import viewModelBase = require("viewmodels/viewModelBase");
import appUrl = require("common/appUrl");

class status extends viewModelBase {

    router: DurandalRootRouter;
    currentRouteTitle: KnockoutComputed<string>;
    appUrls: computedAppUrls;

	constructor() {
        super();
        
        this.appUrls = appUrl.forCurrentDatabase();

        this.router = durandalRouter.createChildRouter()
            .map([
                { route: 'databases/status',                  moduleId: 'viewmodels/statistics',          title: 'Stats',             nav: true, hash: appUrl.forCurrentDatabase().status },
                { route: 'databases/status/logs',             moduleId: 'viewmodels/logs',                title: 'Logs',              nav: true, hash: appUrl.forCurrentDatabase().logs },
                { route: 'databases/status/alerts',           moduleId: 'viewmodels/alerts',              title: 'Alerts',            nav: true, hash: appUrl.forCurrentDatabase().alerts },
                { route: 'databases/status/indexErrors',      moduleId: 'viewmodels/indexErrors',         title: 'Index Errors',      nav: true, hash: appUrl.forCurrentDatabase().indexErrors },
                { route: 'databases/status/replicationStats', moduleId: 'viewmodels/replicationStats',    title: 'Replication Stats', nav: true, hash: appUrl.forCurrentDatabase().replicationStats },
                { route: 'databases/status/userInfo',         moduleId: 'viewmodels/userInfo',            title: 'User Info',         nav: true, hash: appUrl.forCurrentDatabase().userInfo }
			])
            .buildNavigationModel();

        this.currentRouteTitle = ko.computed(() => {
            // Is there a better way to get the active route?
            var activeRoute = this.router.navigationModel().first(r => r.isActive());
            return activeRoute != null ? activeRoute.title : "";
        });
    }
}

export = status;    
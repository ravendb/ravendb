import durandalRouter = require("plugins/router");
import database = require("models/database");
import activeDbViewModelBase = require("viewmodels/activeDbViewModelBase");

class status extends activeDbViewModelBase {

    router: DurandalRootRouter;
    currentRouteTitle: KnockoutComputed<string>;

	constructor() {
        super();
        
        this.router = durandalRouter.createChildRouter()
            .map([
                { route: 'status',                  moduleId: 'viewmodels/statistics',          title: 'Stats',             nav: true },
                { route: 'status/logs',             moduleId: 'viewmodels/logs',                title: 'Logs',              nav: true },
                { route: 'status/alerts',           moduleId: 'viewmodels/alerts',              title: 'Alerts',            nav: true },
                { route: 'status/indexErrors',      moduleId: 'viewmodels/indexErrors',         title: 'Index Errors',      nav: true },
                { route: 'status/replicationStats', moduleId: 'viewmodels/replicationStats',    title: 'Replication Stats', nav: true },
                { route: 'status/userInfo',         moduleId: 'viewmodels/userInfo',            title: 'User Info',         nav: true }
			])
            .buildNavigationModel();
        this.currentRouteTitle = ko.computed(() => {
            // Is there a better way to get the active route?
            var activeRoute = this.router.navigationModel().first<DurandalRouteConfiguration>(r => r.isActive());
            console.log("returning: ", activeRoute != null ? activeRoute.title : "nuttin'");
            return activeRoute != null ? activeRoute.title : "";
        });
    }
}

export = status;    
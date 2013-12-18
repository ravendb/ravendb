import durandalRouter = require("plugins/router");
import database = require("models/database");
import activeDbViewModelBase = require("viewmodels/activeDbViewModelBase");

class status extends activeDbViewModelBase {

    router: DurandalRootRouter;

	constructor() {
        super();

        this.router = durandalRouter.createChildRouter()
            .map([
                { route: 'status',                  moduleId: 'viewmodels/statistics',          title: 'Stats',     type: 'intro', nav: false },
                { route: 'status/statistics',       moduleId: 'viewmodels/statistics',          title: 'Stats', type: 'intro', nav: true },
                { route: 'status/logs',             moduleId: 'viewmodels/logs',                title: 'Logs', nav: true },
                { route: 'status/alerts',           moduleId: 'viewmodels/alerts',              title: 'Alerts', nav: true },
                { route: 'status/indexErrors',      moduleId: 'viewmodels/indexErrors',         title: 'Index Errors', nav: true },
                { route: 'status/replicationStats', moduleId: 'viewmodels/replicationStats',    title: 'Replication Stats', nav: true },
                { route: 'status/userInfo',         moduleId: 'viewmodels/userInfo',            title: 'User Info', type: 'intro', nav: true },
			])
			.buildNavigationModel();
    }
}

export = status;    
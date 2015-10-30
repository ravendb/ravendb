import durandalRouter = require("plugins/router");
import appUrl = require("common/appUrl");

class queryShell {
    router: DurandalRouter;

    constructor() {
        this.router = durandalRouter.createChildRouter()
            .map([
                { route: ['', 'databases/query/index(/:indexNameOrRecentQueryIndex)'], moduleId: 'viewmodels/query', title: 'Query', nav: true },
                { route: 'databases/query/reporting(/:indexName)', moduleId: 'viewmodels/reporting', title: 'Reporting', nav: true }
            ])
            .buildNavigationModel();

        appUrl.mapUnknownRoutes(this.router);
    }
}

export = queryShell;

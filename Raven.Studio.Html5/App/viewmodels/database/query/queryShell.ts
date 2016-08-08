import durandalRouter = require("plugins/router");
import appUrl = require("common/appUrl");

class queryShell {
    router: DurandalRouter;

    constructor() {
        this.router = durandalRouter.createChildRouter()
            .map([
                { route: ['', 'databases/query/index(/:indexNameOrRecentQueryIndex)'], moduleId: 'viewmodels/database/query/query', title: 'Query', nav: true },
                { route: 'databases/query/reporting(/:indexName)', moduleId: 'viewmodels/database/reporting/reporting', title: 'Reporting', nav: true },
                { route: 'databases/query/exploration', moduleId: 'viewmodels/database/exploration/exploration', title: "Data exploration", nav: true }
            ])
            .buildNavigationModel();

        appUrl.mapUnknownRoutes(this.router);
    }

    protected shouldReportUsage(): boolean {
        return false;
    }
}

export = queryShell;

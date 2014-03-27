import durandalRouter = require("plugins/router");

class queryShell {
    router: DurandalRouter;

    constructor() {
        this.router = durandalRouter.createChildRouter()
            .map([
                { route: ['', 'databases/query/index(/:indexNameOrRecentQueryIndex)'], moduleId: 'viewmodels/query', title: 'Query', nav: true },
                { route: 'databases/query/reporting(/:indexName)', moduleId: 'viewmodels/reporting', title: 'Reporting', nav: true }
            ])
            .buildNavigationModel();
    }
}

export = queryShell;
import durandalRouter = require("plugins/router");

class queryShell {
    router: DurandalRouter;

    constructor() {
        this.router = durandalRouter.createChildRouter()
            .map([
                { route: ['', 'query/index(/:indexNameOrRecentQueryIndex)'], moduleId: 'viewmodels/query', title: 'Query', nav: true },
                { route: 'query/reporting(/:indexName)', moduleId: 'viewmodels/reporting', title: 'Reporting', nav: true }
            ])
            .buildNavigationModel();
    }
}

export = queryShell;
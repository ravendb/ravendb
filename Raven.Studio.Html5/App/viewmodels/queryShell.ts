import durandalRouter = require("plugins/router");

class queryShell {
    router: DurandalRouter;

    activate(args) {
        this.router = durandalRouter.createChildRouter()
            .map([
                { route: 'query(/:indexNameOrRecentQueryIndex)', moduleId: 'viewmodels/query', title: 'Query', nav: true },
                { route: 'query/dynamic', moduleId: 'viewmodels/dynamicQuery', title: 'Dynamic Query', nav: true },
                { route: 'query/reporting(/:indexName)', moduleId: 'viewmodels/reporting', title: 'Reporting', nav: true }
            ])
            .buildNavigationModel();
    }
}

export = queryShell;
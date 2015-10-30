import durandalRouter = require("plugins/router");
import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");

class indexesShell extends viewModelBase {
    router: DurandalRootRouter;

    constructor() {
        super();

        this.router = durandalRouter.createChildRouter()
            .map([
                { route: 'databases/indexes', moduleId: 'viewmodels/indexes', title: 'Indexes', nav: true },
                { route: 'databases/indexes/mergeSuggestions', moduleId: 'viewmodels/indexMergeSuggestions', title: 'Index Merge Suggestions', nav: true },
                { route: 'databases/indexes/edit(/:indexName)', moduleId: 'viewmodels/editIndex', title: 'Edit Index', nav: true },
                { route: 'databases/indexes/terms/(:indexName)', moduleId: 'viewmodels/indexTerms', title: 'Terms', nav: true }
            ])
            .buildNavigationModel();

        appUrl.mapUnknownRoutes(this.router);
    }
}

export = indexesShell;

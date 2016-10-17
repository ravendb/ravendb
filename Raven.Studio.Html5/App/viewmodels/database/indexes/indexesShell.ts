import durandalRouter = require("plugins/router");
import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");

class indexesShell extends viewModelBase {
    router: DurandalRootRouter;

    constructor() {
        super();

        this.router = durandalRouter.createChildRouter()
            .map([
                { route: 'databases/indexes', moduleId: 'viewmodels/database/indexes/indexes', title: 'Indexes', nav: true },
                { route: 'databases/indexes/mergeSuggestions', moduleId: 'viewmodels/database/indexes/indexMergeSuggestions', title: 'Index Merge Suggestions', nav: true },
                { route: 'databases/indexes/edit(/:indexName)', moduleId: 'viewmodels/database/indexes/editIndex', title: 'Edit Index', nav: true },
                { route: 'databases/indexes/terms/(:indexName)', moduleId: 'viewmodels/database/indexes/indexTerms', title: 'Terms', nav: true }
            ])
            .buildNavigationModel();

        appUrl.mapUnknownRoutes(this.router);
    }

    protected shouldReportUsage(): boolean {
        return false;
    }
}

export = indexesShell;

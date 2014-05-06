import durandalRouter = require("plugins/router");
import database = require("models/database");
import viewModelBase = require("viewmodels/viewModelBase");
import appUrl = require("common/appUrl");

class indexesShell extends viewModelBase {
    router: DurandalRootRouter;
    currentBreadcrumbTitle: KnockoutComputed<string>;
    indexesUrl = appUrl.forCurrentDatabase().indexes;
    appUrls: computedAppUrls;
    isIndexNameVisible = ko.observable(false);
    indexName = ko.observable();

    constructor() {
        super();

        this.appUrls = appUrl.forCurrentDatabase();

        this.router = durandalRouter.createChildRouter()
            .map([
                { route: 'databases/indexes', moduleId: 'viewmodels/indexes', title: 'Indexes', nav: true },
                { route: 'databases/indexes/edit(/:indexName)', moduleId: 'viewmodels/editIndex', title: 'Edit Index', nav: true },
                { route: 'databases/indexes/terms/(:indexName)', moduleId: 'viewmodels/indexTerms', title: 'Terms', nav: true }
            ])
            .buildNavigationModel();

        this.currentBreadcrumbTitle = ko.computed(() => {
            // Is there a better way to get the active route?
            var activeRoute = this.router.navigationModel().first(r => r.isActive());
            if (activeRoute && activeRoute.title === "Indexes") {
                return "All";
            }

            return activeRoute != null ? activeRoute.title : "";
        });
    }

    activate(indexName?) {
        if (indexName != null) {
            this.indexName(indexName);
            this.isIndexNameVisible(true);
        }
    }
}

export = indexesShell;
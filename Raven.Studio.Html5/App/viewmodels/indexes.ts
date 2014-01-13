import durandalRouter = require("plugins/router");
import database = require("models/database");
import activeDbViewModelBase = require("viewmodels/activeDbViewModelBase");
import appUrl = require("common/appUrl");

class indexes extends activeDbViewModelBase {
    router: DurandalRootRouter;
    currentRouteTitle: KnockoutComputed<string>;
    indexesUrl = appUrl.forCurrentDatabase().indexes;

    constructor() {
        super();

        this.router = durandalRouter.createChildRouter()
            .map([
                { route: 'indexes', moduleId: 'viewmodels/indexesAll', title: 'Index List', nav: true },
                { route: 'indexes/edit(/:indexName)', moduleId: 'viewmodels/editIndex', title: 'Edit Index', nav: true }
            ])
            .buildNavigationModel();

        this.currentRouteTitle = ko.computed(() => {
            // Is there a better way to get the active route?
            var activeRoute = this.router.navigationModel().first(r => r.isActive());
            return activeRoute != null ? activeRoute.title : "";
        });
    }
}

export = indexes;
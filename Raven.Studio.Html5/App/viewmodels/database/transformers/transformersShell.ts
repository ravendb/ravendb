import durandalRouter = require("plugins/router");
import viewModelBase = require("viewmodels/viewModelBase");
import appUrl = require("common/appUrl");

class transformerShell extends viewModelBase {
    router: DurandalRootRouter;
    currentBreadcrumbTitle: KnockoutComputed<string>;
    indexesUrl = appUrl.forCurrentDatabase().indexes;
    appUrls: computedAppUrls;

    constructor() {
        super();

        this.appUrls = appUrl.forCurrentDatabase();

        this.router = durandalRouter.createChildRouter()
            .map([
                { route: 'databases/transformers', moduleId: 'viewmodels/database/transformers/transformers', title: 'Transformers', nav: true },
                { route: 'databases/transformers/edit(/:transformerName)', moduleId: 'viewmodels/database/transformers/editTransformer', title: 'Edit Transformer', nav: true }
                
            ])
            .buildNavigationModel();

        appUrl.mapUnknownRoutes(this.router);

        this.currentBreadcrumbTitle = ko.computed(() => {
            // Is there a better way to get the active route?
            var activeRoute = this.router.navigationModel().first(r => r.isActive());
            if (activeRoute && activeRoute.title === "Transformers") {
                return "All";
            }

            return activeRoute != null ? activeRoute.title : "";
        });
    }

    protected shouldReportUsage(): boolean {
        return false;
    }
}

export = transformerShell;

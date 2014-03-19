import durandalRouter = require("plugins/router");
import database = require("models/database");
import viewModelBase = require("viewmodels/viewModelBase");
import appUrl = require("common/appUrl");

class transformerShell extends viewModelBase {
    router: DurandalRootRouter;
    currentBreadcrumbTitle: KnockoutComputed<string>;
    indexesUrl = appUrl.forCurrentDatabase().indexes;

    constructor() {
        super();

        this.router = durandalRouter.createChildRouter()
            .map([
                { route: 'transformers', moduleId: 'viewmodels/transformers', title: 'Transformers', nav: true },
                { route: 'transformers/edit(/:transformerName)', moduleId: 'viewmodels/editTransformer', title: 'Edit Transformer', nav: true }
                
            ])
            .buildNavigationModel();

        this.currentBreadcrumbTitle = ko.computed(() => {
            // Is there a better way to get the active route?
            var activeRoute = this.router.navigationModel().first(r => r.isActive());
            if (activeRoute && activeRoute.title === "Transformers") {
                return "All";
            }

            return activeRoute != null ? activeRoute.title : "";
        });
    }
}

export = transformerShell;
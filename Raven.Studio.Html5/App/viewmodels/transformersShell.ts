import durandalRouter = require("plugins/router");
import database = require("models/database");
import viewModelBase = require("viewmodels/viewModelBase");
import appUrl = require("common/appUrl");
import editTransformer = require("viewmodels/editTransformer");

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
                { route: 'databases/transformers', moduleId: 'viewmodels/transformers', title: 'Transformers', nav: true },
                { route: 'databases/transformers/edit(/:transformerName)', moduleId: 'viewmodels/editTransformer', title: 'Edit Transformer', nav: true }
                
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

    //todo: implement refresh of all transformers
    modelPolling() {
    }

}

export = transformerShell;
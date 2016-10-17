import durandalRouter = require("plugins/router");
import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");

class configuration extends viewModelBase {

    router: DurandalRootRouter = null;
    appUrls: computedAppUrls;

    activeSubViewTitle: KnockoutComputed<string>;

    constructor() {
        super();

        this.appUrls = appUrl.forCurrentDatabase();

        var typesRoute = { route: ['timeSeries/configuration', 'timeSeries/configuration/types'], moduleId: 'viewmodels/timeSeries/configuration/types', title: 'Types Configuration', nav: true, hash: this.appUrls.timeSeriesConfigurationTypes };

        this.router = durandalRouter.createChildRouter()
            .map([
                typesRoute
            ])
            .buildNavigationModel();

        appUrl.mapUnknownRoutes(this.router);

        this.activeSubViewTitle = ko.computed(() => {
            // Is there a better way to get the active route?
            var activeRoute = this.router.navigationModel().first(r=> r.isActive());
            return activeRoute != null ? activeRoute.title : "";
        });
    }

    protected shouldReportUsage(): boolean {
        return false;
    }
}

export = configuration;

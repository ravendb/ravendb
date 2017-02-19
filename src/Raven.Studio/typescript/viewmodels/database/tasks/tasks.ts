import durandalRouter = require("plugins/router");
import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");

class tasks extends viewModelBase {

    router: DurandalRootRouter = null;
    isOnUserDatabase: KnockoutComputed<boolean>;
    activeSubViewTitle: KnockoutComputed<string>;
    appUrls: computedAppUrls;

    constructor() {
        super();

        this.isOnUserDatabase = ko.computed(() => !!this.activeDatabase());
        this.appUrls = appUrl.forCurrentDatabase();

        this.router = durandalRouter.createChildRouter();
    }

    activate(args: any) {
        super.activate(args);

        var csvImportUrl = ko.computed(() => appUrl.forCsvImport(this.activeDatabase()));

        var routeArray: DurandalRouteConfiguration[] = [
            { route: 'databases/tasks/csvImport', moduleId: 'viewmodels/database/tasks/csvImport', title: 'CSV Import', nav: true, dynamicHash: csvImportUrl }
        ];

        this.router
            .reset()
            .map(routeArray)
            .buildNavigationModel();

        appUrl.mapUnknownRoutes(this.router);

        this.activeSubViewTitle = ko.computed(()=> {
            // Is there a better way to get the active route?
            var activeRoute = this.router.navigationModel().find(r=> r.isActive());
            return activeRoute != null ? activeRoute.title : "";
        });
    }
}

export = tasks;

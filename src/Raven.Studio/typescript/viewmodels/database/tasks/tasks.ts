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

        var importDatabaseUrl = ko.computed(() => appUrl.forImportDatabase(this.activeDatabase()));
        var exportDatabaseUrl = ko.computed(() => appUrl.forExportDatabase(this.activeDatabase()));
        var toggleIndexingUrl = ko.computed(() => appUrl.forToggleIndexing(this.activeDatabase()));
        var setAcknowledgedEtagUrl = ko.computed(() => appUrl.forSetAcknowledgedEtag(this.activeDatabase()));
        var sampleDataUrl = ko.computed(() => appUrl.forSampleData(this.activeDatabase()));
        var csvImportUrl = ko.computed(() => appUrl.forCsvImport(this.activeDatabase()));

        var routeArray: DurandalRouteConfiguration[] = [
            { route: ['databases/tasks', 'databases/tasks/importDatabase'], moduleId: 'viewmodels/database/tasks/importDatabase', title: 'Import Database', nav: true, dynamicHash: importDatabaseUrl },
            { route: 'databases/tasks/exportDatabase', moduleId: 'viewmodels/database/tasks/exportDatabase', title: 'Export Database', nav: true, dynamicHash: exportDatabaseUrl },
            { route: 'databases/tasks/toggleIndexing', moduleId: 'viewmodels/database/tasks/toggleIndexing', title: 'Toggle Indexing', nav: this.activeDatabase().isAdminCurrentTenant(), dynamicHash: toggleIndexingUrl },
            { route: 'databases/tasks/subscriptionsTask', moduleId: 'viewmodels/database/tasks/subscriptionsTask', title: 'Subscriptions', nav: this.activeDatabase().isAdminCurrentTenant(), dynamicHash: setAcknowledgedEtagUrl },
            { route: 'databases/tasks/sampleData', moduleId: 'viewmodels/database/tasks/createSampleData', title: 'Create Sample Data', nav: true, dynamicHash: sampleDataUrl },
            { route: 'databases/tasks/csvImport', moduleId: 'viewmodels/database/tasks/csvImport', title: 'CSV Import', nav: true, dynamicHash: csvImportUrl }
        ];

        this.router = durandalRouter.createChildRouter()
            .map(routeArray)
            .buildNavigationModel();

        appUrl.mapUnknownRoutes(this.router);

        this.activeSubViewTitle = ko.computed(()=> {
            // Is there a better way to get the active route?
            var activeRoute = this.router.navigationModel().first(r=> r.isActive());
            return activeRoute != null ? activeRoute.title : "";
        });
    }

    activate(args: any) {
        super.activate(args);
    }
}

export = tasks;

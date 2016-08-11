import durandalRouter = require("plugins/router");
import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");

class tasks extends viewModelBase {

    router: DurandalRootRouter = null;
    activeSubViewTitle: KnockoutComputed<string>;
    appUrls: computedAppUrls;

    constructor() {
        super();

        this.appUrls = appUrl.forCurrentFilesystem();

        var importFilesystemUrl = ko.computed(() => appUrl.forImportFilesystem(this.activeFilesystem()));
        var exportFilesystemUrl = ko.computed(() => appUrl.forExportFilesystem(this.activeFilesystem()));

        var routeArray: DurandalRouteConfiguration[] = [
            { route: ["filesystems/tasks", "filesystems/tasks/importFilesystem"], moduleId: "viewmodels/filesystem/tasks/importFilesystem", title: "Import File System", nav: true, hash: importFilesystemUrl },
            { route: "filesystems/tasks/exportFilesystem", moduleId: "viewmodels/filesystem/tasks/exportFilesystem", title: "Export File System", nav: true, hash: exportFilesystemUrl }
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

    protected shouldReportUsage(): boolean {
        return false;
    }
}

export = tasks;

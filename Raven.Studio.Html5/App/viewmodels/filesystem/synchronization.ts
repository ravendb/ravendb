import durandalRouter = require("plugins/router");
import filesystem = require("models/filesystem/filesystem");
import viewModelBase = require("viewmodels/viewModelBase");
import appUrl = require("common/appUrl");

class synchronization extends viewModelBase {

    router: DurandalRootRouter;
    static statusRouter: DurandalRouter; //TODO: is it better way of exposing this router to child router?
    currentRouteTitle: KnockoutComputed<string>;
    appUrls: computedAppUrls;

    constructor() {
        super();

        this.appUrls = appUrl.forCurrentFilesystem();

        this.router = durandalRouter.createChildRouter()
            .map([
                { route: 'filesystems/synchronization', moduleId: 'viewmodels/filesystem/synchronizationConflicts', title: 'Conflicts', nav: true, hash: appUrl.forCurrentFilesystem().filesystemSynchronization },
                { route: 'filesystems/synchronization/destinations', moduleId: 'viewmodels/filesystem/synchronizationDestinations', title: 'Destinations', nav: true, hash: appUrl.forCurrentFilesystem().filesystemSynchronizationDestinations }
            ])
            .buildNavigationModel();

        synchronization.statusRouter = this.router;

        appUrl.mapUnknownRoutes(this.router);

        this.currentRouteTitle = ko.computed(() => {
            // Is there a better way to get the active route?
            var activeRoute = this.router.navigationModel().first(r => r.isActive());
            return activeRoute != null ? activeRoute.title : "";
        });
    }
}

export = synchronization;
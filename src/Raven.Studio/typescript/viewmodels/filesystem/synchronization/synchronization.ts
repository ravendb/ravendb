import durandalRouter = require("plugins/router");
import viewModelBase = require("viewmodels/viewModelBase");
import appUrl = require("common/appUrl");

class synchronization extends viewModelBase {
    router: DurandalRootRouter;
    static statusRouter: DurandalRouter; //TODO: is it better way of exposing this router to child router?

    activeSubViewTitle: KnockoutComputed<string>;
    appUrls: computedAppUrls;

    constructor() {
        super();

        this.appUrls = appUrl.forCurrentFilesystem();

        this.router = durandalRouter.createChildRouter()
            .map([
                { route: "filesystems/synchronization", moduleId: "viewmodels/filesystem/synchronization/synchronizationConflicts", title: "Conflicts", nav: true, hash: appUrl.forCurrentFilesystem().filesystemSynchronization },
                { route: "filesystems/synchronization/destinations", moduleId: "viewmodels/filesystem/synchronization/synchronizationDestinations", title: "Destinations", nav: true, hash: appUrl.forCurrentFilesystem().filesystemSynchronizationDestinations },
                { route: "filesystems/synchronization/configuration", moduleId: "viewmodels/filesystem/synchronization/synchronizationConfiguration", title: "Configuration", nav: true, hash: appUrl.forCurrentFilesystem().filesystemSynchronizationConfiguration}
            ])
            .buildNavigationModel();

        synchronization.statusRouter = this.router;

        appUrl.mapUnknownRoutes(this.router);

        this.activeSubViewTitle = ko.computed(() => {
            // Is there a better way to get the active route?
            var activeRoute = this.router.navigationModel().first(r=> r.isActive());
            return activeRoute != null ? activeRoute.title : "";
        });
    }

    canActivate(args: any) {
        return true;
    }
}

export = synchronization;

import viewModelBase = require("viewmodels/viewModelBase");
import appUrl = require("common/appUrl");
import status = require("viewmodels/database/status/status");
import accessHelper = require("viewmodels/shell/accessHelper");

class statusDebug extends viewModelBase {

    router: DurandalRouter;
    currentRouteTitle: KnockoutComputed<string>;

    constructor() {
        super();

        this.router = status.statusRouter.createChildRouter()
            .map([
                { route: 'databases/status/storage', moduleId: 'viewmodels/database/status/storage/statusStorageOnDisk', title: 'On disk', tooltip: "Shows disk usage for active resource", nav: accessHelper.isGlobalAdmin(), hash: appUrl.forCurrentDatabase().statusStorageOnDisk },
                { route: 'databases/status/storage/storageBreakdown', moduleId: 'viewmodels/database/status/storage/statusStorageBreakdown', title: 'Internal storage Breakdown', tooltip: "Shows detailed information about internal storage breakdown", nav: accessHelper.isGlobalAdmin(), hash: appUrl.forCurrentDatabase().statusStorageBreakdown },
                { route: 'databases/status/storage/collections',       moduleId: 'viewmodels/database/status/storage/statusStorageCollections',    title: 'Collections storage',   tooltip: "Shows document counts (VERY SLOW)", nav: true, hash: appUrl.forCurrentDatabase().statusStorageCollections },
            ])
            .buildNavigationModel();

        appUrl.mapUnknownRoutes(this.router);

        this.currentRouteTitle = ko.computed(() => {
            // Is there a better way to get the active route?
            var activeRoute = this.router.navigationModel().first(r => r.isActive());
            return activeRoute != null ? activeRoute.title : "";
        });
    }
}

export = statusDebug;    

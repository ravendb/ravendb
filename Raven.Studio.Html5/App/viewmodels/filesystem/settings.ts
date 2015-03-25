import durandalRouter = require("plugins/router");
import filesystem = require('models/filesystem/filesystem');
import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");

class settings extends viewModelBase {

    router: DurandalRootRouter = null;
    appUrls: computedAppUrls;

    private bundleMap = {  versioning: "Versioning" };
    userDatabasePages = ko.observableArray([]);
    activeSubViewTitle: KnockoutComputed<string>;

    constructor() {
        super();

        this.appUrls = appUrl.forCurrentDatabase();

        var versioningRoute = { route: 'filesystems/settings', moduleId: 'viewmodels/filesystem/versioning', title: 'Versioning', nav: true, hash: appUrl.forCurrentFilesystem().filesystemVersioning };

        this.router = durandalRouter.createChildRouter()
            .map([
                versioningRoute
            ])
            .buildNavigationModel();

        this.router.guardRoute = (instance: Object, instruction: DurandalRouteInstruction) => this.getValidRoute(instance, instruction);

        appUrl.mapUnknownRoutes(this.router);

        this.activeSubViewTitle = ko.computed(() => {
            // Is there a better way to get the active route?
            var activeRoute = this.router.navigationModel().first(r=> r.isActive());
            return activeRoute != null ? activeRoute.title : "";
        });
    }

    /**
    * Checks whether the route can be navigated to. Returns true if it can be navigated to, or a redirect URI if it can't be navigated to.
    * This is used for preventing a navigating to system-only pages when the current databagse is non-system, and vice-versa.
    */
    getValidRoute(instance: Object, instruction: DurandalRouteInstruction): any {
        var fs: filesystem = this.activeFilesystem();
        var pathArr = instruction.fragment.split('/');
        var bundelName = pathArr[pathArr.length - 1].toLowerCase();
        var isLegalBundelName = (this.bundleMap[bundelName] != undefined);
        var isBundleExists = this.userDatabasePages.indexOf(this.bundleMap[bundelName]) > -1;
        
        if (isLegalBundelName && isBundleExists == false) {
            return appUrl.forCurrentDatabase().filesystemSettings();
        }

        return true;
    }

    activate(args) {
        super.activate(args);

        this.userDatabasePages([]);
        var fs: filesystem = this.activeFilesystem();
        var bundles: string[] = fs.activeBundles();

        bundles.forEach((bundle: string) => {
            var bundleName = this.bundleMap[bundle.toLowerCase()];
            if (bundleName != undefined) {
                this.userDatabasePages.push(bundleName);
            }
        });
    }

    routeIsVisible(route: DurandalRouteConfiguration) {
        var bundleTitle = route.title;

        if (this.userDatabasePages.indexOf(bundleTitle) !== -1) {
            return true;
        }

        return false;
    }
}

export = settings;

import durandalRouter = require("plugins/router");
import database = require("models/database");
import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import getDatabaseSettingsCommand = require("commands/getDatabaseSettingsCommand");
import alertType = require("common/alertType");
import alertArgs = require("common/alertArgs");

class settings extends viewModelBase {

    router: DurandalRootRouter = null;
    isOnSystemDatabase: KnockoutComputed<boolean>;
    isOnUserDatabase: KnockoutComputed<boolean>;
    appUrls: computedAppUrls;

    bundleMap = { quotas: "Quotas", replication: "Replication", sqlreplication: "SQL Replication", versioning: "Versioning", periodicexport: "Periodic Export", scriptedindexresults: "Scripted Index"};
    userDatabasePages = ko.observableArray(["Database Settings", "Custom Functions"]);
    systemDatabasePages = ["API Keys", "Windows Authentication"];
    activeSubViewTitle: KnockoutComputed<string>;

    isEditingSqlReplication(navigationalModel:any, curNavHash:any) {
        var activeRoute = navigationalModel.first(r=> r.isActive());
        if (!!activeRoute && !!curNavHash && !!activeRoute.hash) {
            return curNavHash.indexOf('databases/settings/sqlReplication') >= 0 &&
                activeRoute.route.indexOf('databases/settings/editSqlReplication') >= 0;
        } else {
            return false;
        }

        //($root.router.navigationModel.first(function(x){return x.isActive}).hash().indexOf('databases/settings/editSqlReplication/')>=0)
    }

    constructor() {
        super();

        this.appUrls = appUrl.forCurrentDatabase();

        this.isOnSystemDatabase = ko.computed(() => this.activeDatabase() && this.activeDatabase().isSystem);
        this.isOnUserDatabase = ko.computed(() => this.activeDatabase() && !this.isOnSystemDatabase());

        var apiKeyRoute = { route: 'databases/settings/apiKeys', moduleId: 'viewmodels/apiKeys', title: 'API Keys', nav: true, hash: appUrl.forApiKeys() };
        var windowsAuthRoute = { route: 'databases/settings/windowsAuth', moduleId: 'viewmodels/windowsAuth', title: 'Windows Authentication', nav: true, hash: appUrl.forWindowsAuth() };
        var databaseSettingsRoute = { route: ['databases/settings', 'databases/settings/databaseSettings'], moduleId: 'viewmodels/databaseSettings', title: 'Database Settings', nav: true, hash: appUrl.forCurrentDatabase().databaseSettings };
        var quotasRoute = { route: 'databases/settings/quotas', moduleId: 'viewmodels/quotas', title: 'Quotas', nav: true, hash: appUrl.forCurrentDatabase().quotas };
        var replicationsRoute = { route: 'databases/settings/replication', moduleId: 'viewmodels/replications', title: 'Replication', nav: true, hash: appUrl.forCurrentDatabase().replications };
        var sqlReplicationsRoute = { route: 'databases/settings/sqlReplication', moduleId: 'viewmodels/sqlReplications', title: 'SQL Replication', nav: true, hash: appUrl.forCurrentDatabase().sqlReplications };
        var editsqlReplicationsRoute = { route: 'databases/settings/editSqlReplication(/:sqlReplicationName)', moduleId: 'viewmodels/editSqlReplication', title: 'Edit SQL Replication', nav: true, hash: appUrl.forCurrentDatabase().editSqlReplication };
        var versioningRoute = { route: 'databases/settings/versioning', moduleId: 'viewmodels/versioning', title: 'Versioning', nav: true, hash: appUrl.forCurrentDatabase().versioning };
        var periodicExportRoute = { route: 'databases/settings/periodicExports', moduleId: 'viewmodels/periodicExport', title: 'Periodic Export', nav: true, hash: appUrl.forCurrentDatabase().periodicExport };
        //var scriptedIndexesRoute = { route: 'databases/settings/scriptedIndex', moduleId: 'viewmodels/scriptedIndexes', title: 'Scripted Index', nav: true, hash: appUrl.forCurrentDatabase().scriptedIndexes };
        var customFunctionsEditorRoute = { route: 'databases/settings/customFunctionsEditor', moduleId: 'viewmodels/customFunctionsEditor', title: 'Custom Functions', nav: true, hash: appUrl.forCurrentDatabase().customFunctionsEditor };
        

        this.router = durandalRouter.createChildRouter()
            .map([
                apiKeyRoute,
                windowsAuthRoute,
                databaseSettingsRoute,
                quotasRoute,
                replicationsRoute,
                sqlReplicationsRoute,
                editsqlReplicationsRoute,
                versioningRoute,
                periodicExportRoute,
                //scriptedIndexesRoute,
                customFunctionsEditorRoute
            ])
            .buildNavigationModel();

        this.router.guardRoute = (instance: Object, instruction: DurandalRouteInstruction) => this.getValidRoute(instance, instruction);

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
        var pathArr = instruction.fragment.split('/');
        var bundelName = pathArr[pathArr.length - 1].toLowerCase();
        var isLegalBundelName = (this.bundleMap[bundelName] != undefined);
        var isBundleExists = this.userDatabasePages.indexOf(this.bundleMap[bundelName]) >= 0;
        var isSystemDbOnlyPath = instruction.fragment.indexOf("windowsAuth") >= 0 || instruction.fragment.indexOf("apiKeys") >= 0 || instruction.fragment === "settings";
        var isUserDbOnlyPath = !isSystemDbOnlyPath;

        if ((isSystemDbOnlyPath && !this.activeDatabase().isSystem)){
            return appUrl.forCurrentDatabase().databaseSettings();
        } else if (isUserDbOnlyPath && this.activeDatabase().isSystem) {
            return appUrl.forApiKeys();
        } else if (isUserDbOnlyPath && isLegalBundelName && !isBundleExists) {
            return appUrl.forCurrentDatabase().databaseSettings();
        }

        return true;
    }

    activate(args) {
        super.activate(args);
        this.userDatabasePages(["Database Settings", "Custom Functions"]);
        if (args) {
            var canActivateResult = $.Deferred();
            var db = this.activeDatabase();
            var self = this;
            new getDatabaseSettingsCommand(db)
                .execute()
                .done(document => {
                    var documentSettings = document.Settings["Raven/ActiveBundles"];
                    if (documentSettings != undefined) {
                        var arr = documentSettings.split(';');

                        for (var i = 0; i < arr.length; i++) {
                            var bundleName = self.bundleMap[arr[i].toLowerCase()];
                            if (bundleName != undefined) {
                                self.userDatabasePages.push(bundleName);
                            }
                        }
                    }

                canActivateResult.resolve({ can: true });
                });
            return canActivateResult;
        } else {
            return $.Deferred().resolve({ can: true });
        }
    }

    routeIsVisible(route: DurandalRouteConfiguration) {
        var bundleTitle = route.title;

        if (this.isOnUserDatabase() && (this.userDatabasePages.indexOf(bundleTitle) !== -1)) {
            // Database Settings, Quotas, Replication, SQL Replication, Versioning, Periodic Export and Scripted Index are visible only when we're on a user database.
            return true;
        }
        if (this.isOnSystemDatabase() && (this.systemDatabasePages.indexOf(bundleTitle) !== -1)) {
            // API keys and Windows Auth are visible only when we're on the system database.
            return true;
        }

        return false;
    }
}

export = settings;

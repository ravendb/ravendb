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

    bundleMap = { quotas: "Quotas", replication: "Replication", sqlreplication: "SQL Replication", versioning: "Versioning", periodicbackups: "Periodic Backup", scriptedindexresults: "Scripted Index", scriptedindex: "Scripted Index"};
    userDatabasePages = ko.observableArray(["Database Settings"]);
    systemDatabasePages = ["API Keys", "Windows Authentication"];

    constructor() {
        super();

        this.appUrls = appUrl.forCurrentDatabase();

        this.isOnSystemDatabase = ko.computed(() => this.activeDatabase() && this.activeDatabase().isSystem);
        this.isOnUserDatabase = ko.computed(() => this.activeDatabase() && !this.isOnSystemDatabase());

        var apiKeyRoute = { route: 'databases/settings/apiKeys', moduleId: 'viewmodels/apiKeys', title: 'API Keys', nav: true, hash: appUrl.forApiKeys() };
        var windowsAuthRoute = { route: 'databases/settings/windowsAuth', moduleId: 'viewmodels/windowsAuth', title: 'Windows Authentication', nav: true, hash: appUrl.forWindowsAuth() };
        var databaseSettingsRoute = { route: ['databases/settings', 'databases/settings/databaseSettings'], moduleId: 'viewmodels/databaseSettings', title: 'Database Settings', nav: true, hash: appUrl.forCurrentDatabase().databaseSettings };
        //var quotasRoute = { route: 'settings/quotas', moduleId: 'viewmodels/quotas', title: 'Quotas', nav: true, hash: appUrl.forCurrentDatabase().quotas };
        var replicationsRoute = { route: 'databases/settings/replication', moduleId: 'viewmodels/replications', title: 'Replication', nav: true, hash: appUrl.forCurrentDatabase().replications };
        var sqlReplicationsRoute = { route: 'databases/settings/sqlReplication', moduleId: 'viewmodels/sqlReplications', title: 'SQL Replication', nav: true, hash: appUrl.forCurrentDatabase().sqlReplications };
        //var versioningRoute = { route: 'settings/versioning', moduleId: 'viewmodels/versioning', title: 'Versioning', nav: true, hash: appUrl.forCurrentDatabase().versioning };
        var periodicBackupRoute = { route: 'databases/settings/periodicBackups', moduleId: 'viewmodels/periodicBackup', title: 'Periodic Backup', nav: true, hash: appUrl.forCurrentDatabase().periodicBackup };
        var scriptedIndexesRoute = { route: 'databases/settings/scriptedIndex', moduleId: 'viewmodels/scriptedIndexes', title: 'Scripted Index', nav: true, hash: appUrl.forCurrentDatabase().scriptedIndexes };

        this.router = durandalRouter.createChildRouter()
            .map([
                apiKeyRoute,
                windowsAuthRoute,
                databaseSettingsRoute,
                //quotasRoute,
                replicationsRoute,
                sqlReplicationsRoute,
                //versioningRoute,
                periodicBackupRoute,
                scriptedIndexesRoute
            ])
            .buildNavigationModel();

        this.router.guardRoute = (instance: Object, instruction: DurandalRouteInstruction) => this.getValidRoute(instance, instruction);
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
        this.userDatabasePages(["Database Settings"]);
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
            // Database Settings, Quotas, Replication, SQL Replication, Versioning, Periodic Backup and Scripted Index are visible only when we're on a user database.
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
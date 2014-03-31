import durandalRouter = require("plugins/router");
import database = require("models/database");
import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import getDocumentWithMetadataCommand = require("commands/getDocumentWithMetadataCommand");
import alertType = require("common/alertType");
import alertArgs = require("common/alertArgs");

class settings extends viewModelBase {

    router: DurandalRootRouter = null;
    isOnSystemDatabase: KnockoutComputed<boolean>;
    isOnUserDatabase: KnockoutComputed<boolean>;
    appUrls: computedAppUrls;

    userDatabasePages = ko.observableArray(["Periodic Backup", "Database Settings", "Scripted Index"]);

    constructor() {
        super();

        this.appUrls = appUrl.forCurrentDatabase();

        this.isOnSystemDatabase = ko.computed(() => this.activeDatabase() && this.activeDatabase().isSystem);
        this.isOnUserDatabase = ko.computed(() => this.activeDatabase() && !this.isOnSystemDatabase());

        var apiKeyRoute = { route: ['databases/settings', 'databases/settings/apiKeys'], moduleId: 'viewmodels/apiKeys', title: 'API Keys', nav: true, hash: appUrl.forApiKeys() };
        var windowsAuthRoute = { route: 'databases/settings/windowsAuth', moduleId: 'viewmodels/windowsAuth', title: 'Windows Authentication', nav: true, hash: appUrl.forWindowsAuth() };
        var databaseSettingsRoute = { route: 'databases/settings/databaseSettings', moduleId: 'viewmodels/databaseSettings', title: 'Database Settings', nav: true, hash: appUrl.forCurrentDatabase().databaseSettings };
        var periodicBackupRoute = { route: 'databases/settings/periodicBackup', moduleId: 'viewmodels/periodicBackup', title: 'Periodic Backup', nav: true, hash: appUrl.forCurrentDatabase().periodicBackup };
        var replicationsRoute = { route: 'databases/settings/replication', moduleId: 'viewmodels/replications', title: 'Replication', nav: true, hash: appUrl.forCurrentDatabase().replications };
        var sqlReplicationsRoute = { route: 'databases/settings/sqlReplication', moduleId: 'viewmodels/sqlReplications', title: 'SQL Replication', nav: true, hash: appUrl.forCurrentDatabase().sqlReplications };
        var scriptedIndexesRoute = { route: 'databases/settings/scriptedIndex', moduleId: 'viewmodels/scriptedIndexes', title: 'Scripted Index', nav: true, hash: appUrl.forCurrentDatabase().scriptedIndexes };
        this.router = durandalRouter.createChildRouter()
            .map([
                apiKeyRoute,
                windowsAuthRoute,
                databaseSettingsRoute,
                periodicBackupRoute,
                replicationsRoute,
                sqlReplicationsRoute,
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

        var isSystemDbOnlyPath = instruction.fragment.indexOf("windowsAuth") >= 0 || instruction.fragment.indexOf("apiKeys") >= 0 || instruction.fragment === "settings";
        var isUserDbOnlyPath = !isSystemDbOnlyPath;
        if (isSystemDbOnlyPath && !this.activeDatabase().isSystem) {
            return appUrl.forCurrentDatabase().databaseSettings();
        } else if (isUserDbOnlyPath && this.activeDatabase().isSystem) {
            return appUrl.forApiKeys();
        }

        return true;
    }

    activate(args) {
        super.activate(args);
        if (args) {
            var canActivateResult = $.Deferred();
            var db = this.activeDatabase();
            var documentId = "Raven/Databases/" + db.name;
            var systemDatabase = appUrl.getSystemDatabase();
            var self = this;
            new getDocumentWithMetadataCommand(documentId, systemDatabase)
                .execute()
                .done(document => {
                    var arr = document.Settings["Raven/ActiveBundles"].split(';');
                    self.userDatabasePages.remove("Replication");
                    self.userDatabasePages.remove("SQL Replication");
                    if (jQuery.inArray("Replication", arr) != -1) {
                        self.userDatabasePages.push("Replication");
                    }
                    if (jQuery.inArray("SqlReplication", arr) != -1) {
                        self.userDatabasePages.push("SQL Replication");
                    }
                    
                    canActivateResult.resolve({ can: true });
                });
            return canActivateResult;
        } else {
            return $.Deferred().resolve({ can: true });
        }
    }

    routeIsVisible(route: DurandalRouteConfiguration) {
        if (this.userDatabasePages.indexOf(route.title) !== -1) {
            // Periodic backup, database settings, and replication are visible only when we're on a user database.
            return this.isOnUserDatabase();
        } else {
            // API keys and Windows Auth are visible only when we're on the system database.
            return this.isOnSystemDatabase();
        }
    }
}

export = settings;
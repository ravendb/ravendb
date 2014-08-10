import durandalRouter = require("plugins/router");
import database = require("models/database");
import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import getDatabaseSettingsCommand = require("commands/getDatabaseSettingsCommand");

class adminSettings extends viewModelBase {

    router: DurandalRootRouter = null;
    activeSubViewTitle: KnockoutComputed<string>;
    docsForSystemUrl: string;

    constructor() {
        super();

        this.docsForSystemUrl = appUrl.forDocuments(null, new database("<system>"));

        var apiKeyRoute = { route: ['admin/settings', 'admin/settings/apiKeys'], moduleId: 'viewmodels/apiKeys', title: 'API Keys', nav: true, hash: appUrl.forApiKeys() };
        var windowsAuthRoute = { route: 'admin/settings/windowsAuth', moduleId: 'viewmodels/windowsAuth', title: 'Windows Authentication', nav: true, hash: appUrl.forWindowsAuth() };
        var backupDatabaseRoute = { route: 'admin/settings/backupDatabase', moduleId: 'viewmodels/backupDatabase', title: 'Backup Database', nav: true, hash: appUrl.forBackupDatabase() };
        var restoreDatabaseRoute = { route: 'admin/settings/restoreDatabase', moduleId: 'viewmodels/restoreDatabase', title: 'Restore Database', nav: true, hash: appUrl.forRestoreDatabase() };
        var adminLogsRoute = { route: 'admin/settings/restoreDatabase', moduleId: 'viewmodels/restoreDatabase', title: 'Admin Logs', nav: true, hash: appUrl.forAdminLogs() };
        var trafficWatchRoute = { route: 'admin/settings/restoreDatabase', moduleId: 'viewmodels/restoreDatabase', title: 'Traffic Watch', nav: true, hash: appUrl.forTrafficWatch() };
        var studioConfigRoute = { route: 'admin/settings/studioConfig', moduleId: 'viewmodels/studioConfig', title: 'Studio Config', nav: true, hash: appUrl.forStudioConfig() };

        this.router = durandalRouter.createChildRouter()
            .map([
                apiKeyRoute,
                windowsAuthRoute,
                backupDatabaseRoute,
                restoreDatabaseRoute,
                adminLogsRoute,
                trafficWatchRoute,
                studioConfigRoute
            ])
            .buildNavigationModel();

        appUrl.mapUnknownRoutes(this.router);

        this.activeSubViewTitle = ko.computed(() => {
            // Is there a better way to get the active route?
            var activeRoute = this.router.navigationModel().first(r=> r.isActive());
            return activeRoute != null ? activeRoute.title : "";
        });
    }

    canActivate(args): any {
        return true;
    }

    navigateToSystemDatabase() {
        var db: database = appUrl.getSystemDatabase()
        db.activate();
        var url = appUrl.forDocuments(null, db);
        this.navigate(url);
    }
}

export = adminSettings;

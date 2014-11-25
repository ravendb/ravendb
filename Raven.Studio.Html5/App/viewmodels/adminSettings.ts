import durandalRouter = require("plugins/router");
import database = require("models/database");
import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");

class adminSettings extends viewModelBase {

    router: DurandalRootRouter = null;
    activeSubViewTitle: KnockoutComputed<string>;
    docsForSystemUrl: string;

    constructor() {
        super();

        this.docsForSystemUrl = appUrl.forDocuments(null, new database("<system>"));

        var apiKeyRoute = { route: ['admin/settings', 'admin/settings/apiKeys'], moduleId: 'viewmodels/apiKeys', title: 'API Keys', nav: true, hash: appUrl.forApiKeys() };
        var windowsAuthRoute = { route: 'admin/settings/windowsAuth', moduleId: 'viewmodels/windowsAuth', title: 'Windows Authentication', nav: true, hash: appUrl.forWindowsAuth() };
        var backupRoute = { route: 'admin/settings/backup', moduleId: 'viewmodels/backup', title: 'Backup', nav: true, hash: appUrl.forBackup() };
        var compactDatabaseRoute = { route: 'admin/settings/compactDatabase', moduleId: 'viewmodels/compactDatabase', title: 'Compact Database', nav: true, hash: appUrl.forCompactDatabase() };
        var restoreDatabaseRoute = { route: 'admin/settings/restoreDatabase', moduleId: 'viewmodels/restoreDatabase', title: 'Restore Database', nav: true, hash: appUrl.forRestoreDatabase() };
        var compactFilesystemRoute = { route: 'admin/settings/compactFilesystem', moduleId: 'viewmodels/filesystem/compactFilesystem', title: 'Compact Filesystem', nav: true, hash: appUrl.forCompactFilesystem() };
        var restoreFilesystemRoute = { route: 'admin/settings/restoreFilesystem', moduleId: 'viewmodels/filesystem/restoreFilesystem', title: 'Restore Filesystem', nav: true, hash: appUrl.forRestoreFilesystem() };
        var adminLogsRoute = { route: 'admin/settings/adminLogs', moduleId: 'viewmodels/adminLogs', title: 'Admin Logs', nav: true, hash: appUrl.forAdminLogs() };
        var trafficWatchRoute = { route: 'admin/settings/trafficWatch', moduleId: 'viewmodels/trafficWatch', title: 'Traffic Watch', nav: true, hash: appUrl.forTrafficWatch() };
        var debugInfoRoute = { route: 'admin/settings/debugInfo', moduleId: 'viewmodels/infoPackage', title: 'Gather Debug Info', nav: true, hash: appUrl.forDebugInfo() };
        var ioTestRoute = { route: 'admin/settings/ioTest', moduleId: 'viewmodels/ioTest', title: 'IO Test', nav: true, hash: appUrl.forIoTest() };
        var studioConfigRoute = { route: 'admin/settings/studioConfig', moduleId: 'viewmodels/studioConfig', title: 'Studio Config', nav: true, hash: appUrl.forStudioConfig() };

        this.router = durandalRouter.createChildRouter()
            .map([
                apiKeyRoute,
                windowsAuthRoute,
                backupRoute,
                compactDatabaseRoute,
                restoreDatabaseRoute,
                compactFilesystemRoute, 
                restoreFilesystemRoute,
                adminLogsRoute,
                trafficWatchRoute,
                debugInfoRoute,
                ioTestRoute,
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
        this.promptNavSystemDb(true).done(() => {
            var db: database = appUrl.getSystemDatabase();
            db.activate();
            var url = appUrl.forDocuments(null, db);
            this.navigate(url);
        });
    }
}

export = adminSettings;

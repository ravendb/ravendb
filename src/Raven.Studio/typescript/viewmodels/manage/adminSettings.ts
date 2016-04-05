import durandalRouter = require("plugins/router");
import database = require("models/resources/database");
import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import shell = require("viewmodels/shell");
import license = require("models/auth/license");

class adminSettings extends viewModelBase {

    router: DurandalRootRouter = null;
    static adminSettingsRouter: DurandalRouter; //TODO: is it better way of exposing this router to child router?
    activeSubViewTitle: KnockoutComputed<string>;
    docsForSystemUrl: string;
    isSystemDatabaseForbidden = ko.observable<boolean>();

    constructor() {
        super();

        this.isSystemDatabaseForbidden((shell.isGlobalAdmin() || shell.canReadWriteSettings() || shell.canReadSettings()) === false);
        this.docsForSystemUrl = appUrl.forDocuments(null, appUrl.getSystemDatabase());

        var licenseInformation = { route: 'admin/settings/licenseInformation', moduleId: 'viewmodels/manage/licenseInformation', title: 'License Information', nav: true, hash: appUrl.forLicenseInformation() };
        var apiKeyRoute = { route: ['admin/settings', 'admin/settings/apiKeys'], moduleId: 'viewmodels/manage/apiKeys', title: 'API Keys', nav: true, hash: appUrl.forApiKeys() };
        var windowsAuthRoute = { route: 'admin/settings/windowsAuth', moduleId: 'viewmodels/manage/windowsAuth', title: 'Windows Authentication', nav: true, hash: appUrl.forWindowsAuth() };
        var clusterRoute = { route: 'admin/settings/cluster', moduleId: "viewmodels/manage/cluster", title: "Cluster", nav: true, hash: appUrl.forCluster() };
        var globalConfigRoute = { route: 'admin/settings/globalConfig*details', moduleId: 'viewmodels/manage/globalConfig/globalConfig', title: 'Global Configuration', nav: true, hash: appUrl.forGlobalConfig() };
        var serverSmuggling = { route: "admin/settings/serverSmuggling", moduleId: "viewmodels/manage/serverSmuggling", title: "Server Smuggling", nav: true, hash: appUrl.forServerSmugging() };
        var backupRoute = { route: 'admin/settings/backup', moduleId: 'viewmodels/manage/backup', title: 'Backup', nav: true, hash: appUrl.forBackup() };
        var compactRoute = { route: 'admin/settings/compact', moduleId: 'viewmodels/manage/compact', title: 'Compact', nav: true, hash: appUrl.forCompact() };
        var restoreRoute = { route: 'admin/settings/restore', moduleId: 'viewmodels/manage/restore', title: 'Restore', nav: true, hash: appUrl.forRestore() };
        var adminLogsRoute = { route: 'admin/settings/adminLogs', moduleId: 'viewmodels/manage/adminLogs', title: 'Admin Logs', nav: true, hash: appUrl.forAdminLogs() };
        var topologyRoute = { route: 'admin/settings/topology', moduleId: 'viewmodels/manage/topology', title: 'Server topology', nav: true, hash: appUrl.forServerTopology() };
        var trafficWatchRoute = { route: 'admin/settings/trafficWatch', moduleId: 'viewmodels/manage/trafficWatch', title: 'Traffic Watch', nav: true, hash: appUrl.forTrafficWatch() };
        var debugInfoRoute = { route: 'admin/settings/debugInfo', moduleId: 'viewmodels/manage/infoPackage', title: 'Gather Debug Info', nav: true, hash: appUrl.forDebugInfo() };
        var ioTestRoute = { route: 'admin/settings/ioTest', moduleId: 'viewmodels/manage/ioTest', title: 'IO Test', nav: true, hash: appUrl.forIoTest() };
        var diskIoViewerRoute = { route: 'admin/settings/diskIoViewer', moduleId: 'viewmodels/manage/diskIoViewer', title: 'Disk IO Viewer', nav: true, hash: appUrl.forDiskIoViewer() };
        var consoleRoute = { route: 'admin/settings/console', moduleId: "viewmodels/manage/console", title: "Administrator JS Console", nav: true, hash: appUrl.forAdminJsConsole() };
        var studioConfigRoute = { route: 'admin/settings/studioConfig', moduleId: 'viewmodels/manage/studioConfig', title: 'Studio Config', nav: true, hash: appUrl.forStudioConfig() };
        var hotSpareRoute = { route: 'admin/settings/hotSpare', moduleId: 'viewmodels/manage/hotSpare', title: 'Hot Spare', nav: true, hash: appUrl.forHotSpare() };

        var routes = [
            apiKeyRoute,
            windowsAuthRoute,
            clusterRoute,
            globalConfigRoute,
            serverSmuggling,
            backupRoute,
            compactRoute,
            restoreRoute,
            adminLogsRoute,
            topologyRoute,
            trafficWatchRoute,
            licenseInformation,
            debugInfoRoute,
            ioTestRoute,
            diskIoViewerRoute,
            consoleRoute,
            studioConfigRoute
        ];
        if (!!license && license.licenseStatus().Attributes.hotSpare === "true")
            routes.push(hotSpareRoute);
        if (!shell.has40Features()) {
            routes.remove(clusterRoute);
        }

        this.router = durandalRouter.createChildRouter()
            .map(routes)
            .buildNavigationModel();

        adminSettings.adminSettingsRouter = this.router;
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
}

export = adminSettings;

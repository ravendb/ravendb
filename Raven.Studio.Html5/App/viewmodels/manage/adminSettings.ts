import durandalRouter = require("plugins/router");
import database = require("models/resources/database");
import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import shell = require("viewmodels/shell");
import license = require("models/auth/license");
import settingsAccessAuthorizer = require("common/settingsAccessAuthorizer");

class adminSettings extends viewModelBase {

    router: DurandalRootRouter = null;
    static adminSettingsRouter: DurandalRouter; //TODO: is it better way of exposing this router to child router?
    activeSubViewTitle: KnockoutComputed<string>;
    docsForSystemUrl: string;
    isSystemDatabaseForbidden = ko.observable<boolean>();

    settingsAccess = new settingsAccessAuthorizer();

    constructor() {
        super();
        this.router = durandalRouter.createChildRouter()
            .makeRelative({
                fromParent: true,
                moduleId: 'viewmodels/manage'
            });
    }

    canActivate(args): any {
        return true;
    }

    activate(args) {
        super.activate(args);
        if (!license.licenseStatus()) {
            // we want to make sure license information is loaded before building route info
            return shell.fetchLicenseStatus()
                .done(() => this.init());
        } else {
            this.init();
        }
    }

    init() {
        this.docsForSystemUrl = appUrl.forDocuments(null, appUrl.getSystemDatabase());
        this.isSystemDatabaseForbidden((shell.isGlobalAdmin() || shell.canReadWriteSettings() || shell.canReadSettings()) === false);

        var canReadOrWrite = this.settingsAccess.canReadOrWrite();
        var isGlobalAdmin = shell.isGlobalAdmin();

        var apiKeyRoute = { route: ['', 'apiKeys'], moduleId: 'viewmodels/manage/apiKeys', title: 'API Keys', nav: true, hash: appUrl.forApiKeys(), enabled: canReadOrWrite };
        var windowsAuthRoute = { route: 'windowsAuth', moduleId: 'viewmodels/manage/windowsAuth', title: 'Windows Authentication', nav: true, hash: appUrl.forWindowsAuth(), enabled: canReadOrWrite };
        var clusterRoute = { route: 'cluster', moduleId: "viewmodels/manage/cluster", title: "Cluster", nav: true, hash: appUrl.forCluster(), enabled: canReadOrWrite };
        var globalConfigRoute = { route: 'globalConfig*details', moduleId: 'viewmodels/manage/globalConfig/globalConfig', title: 'Global Configuration', nav: true, hash: appUrl.forGlobalConfig(), enabled: canReadOrWrite };
        var serverSmuggling = { route: "serverSmuggling", moduleId: "viewmodels/manage/serverSmuggling", title: "Server Smuggling", nav: true, hash: appUrl.forServerSmugging(), enabled: isGlobalAdmin };
        var backupRoute = { route: 'backup', moduleId: 'viewmodels/manage/backup', title: 'Backup', nav: true, hash: appUrl.forBackup(), enabled: isGlobalAdmin };
        var compactRoute = { route: 'compact', moduleId: 'viewmodels/manage/compact', title: 'Compact', nav: true, hash: appUrl.forCompact(), enabled: isGlobalAdmin };
        var restoreRoute = { route: 'restore', moduleId: 'viewmodels/manage/restore', title: 'Restore', nav: true, hash: appUrl.forRestore(), enabled: isGlobalAdmin };
        var adminLogsRoute = { route: 'adminLogs', moduleId: 'viewmodels/manage/adminLogs', title: 'Admin Logs', nav: true, hash: appUrl.forAdminLogs(), enabled: isGlobalAdmin };
        var topologyRoute = { route: 'topology', moduleId: 'viewmodels/manage/topology', title: 'Server Topology', nav: true, hash: appUrl.forServerTopology(), enabled: isGlobalAdmin };
        var trafficWatchRoute = { route: 'trafficWatch', moduleId: 'viewmodels/manage/trafficWatch', title: 'Traffic Watch', nav: true, hash: appUrl.forTrafficWatch(), enabled: isGlobalAdmin };
        var licenseInformation = { route: 'licenseInformation', moduleId: 'viewmodels/manage/licenseInformation', title: 'License Information', nav: true, hash: appUrl.forLicenseInformation(), enabled: canReadOrWrite };
        var debugInfoRoute = { route: 'debugInfo', moduleId: 'viewmodels/manage/infoPackage', title: 'Gather Debug Info', nav: true, hash: appUrl.forDebugInfo(), enabled: isGlobalAdmin };
        var ioTestRoute = { route: 'ioTest', moduleId: 'viewmodels/manage/ioTest', title: 'IO Test', nav: true, hash: appUrl.forIoTest(), enabled: isGlobalAdmin };
        var diskIoViewerRoute = { route: 'diskIoViewer', moduleId: 'viewmodels/manage/diskIoViewer', title: 'Disk IO Viewer', nav: true, hash: appUrl.forDiskIoViewer(), enabled: canReadOrWrite };
        var consoleRoute = { route: 'console', moduleId: "viewmodels/manage/console", title: "Administrator JS Console", nav: true, hash: appUrl.forAdminJsConsole(), enabled: isGlobalAdmin };
        var studioConfigRoute = { route: 'studioConfig', moduleId: 'viewmodels/manage/studioConfig', title: 'Studio Config', nav: true, hash: appUrl.forStudioConfig(), enabled: canReadOrWrite };
        var hotSpareRoute = { route: 'hotSpare', moduleId: 'viewmodels/manage/hotSpare', title: 'Hot Spare', nav: true, hash: appUrl.forHotSpare(), enabled: isGlobalAdmin };

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

        if (license.licenseStatus().Attributes.hotSpare === "true")
            routes.push(hotSpareRoute);

        this.router = this.router
            .reset()
            .map(routes)
            .buildNavigationModel();

        adminSettings.adminSettingsRouter = this.router;
        appUrl.mapUnknownRoutes(this.router);

        this.activeSubViewTitle = ko.computed(() => {
            // Is there a better way to get the active route?
            var activeRoute = this.router.navigationModel().first(r => r.isActive());
            return activeRoute != null ? activeRoute.title : "";
        });
    }

    navigateToSystemDatabase() {
        this.promptNavSystemDb(true).done(() => {
            var db: database = appUrl.getSystemDatabase();
            db.activate();
            var url = appUrl.forDocuments(null, db);
            this.navigate(url);
        });
    }

    protected shouldReportUsage(): boolean {
        return false;
    }
}

export = adminSettings;

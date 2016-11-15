import appUrl = require("common/appUrl");
import settingsAccessAuthorizer = require("common/settingsAccessAuthorizer");
import accessHelper = require("viewModels/shell/accessHelper");
import intermediateMenuItem = require("common/shell/menu/intermediateMenuItem");
import leafMenuItem = require("common/shell/menu/leafMenuItem");

export = getManageServerMenuItem;

function getManageServerMenuItem() {
    let canReadOrWrite = settingsAccessAuthorizer.canReadOrWrite;
    var items: menuItem[] = [
        new intermediateMenuItem("Global config", [
            new leafMenuItem({
                route: "admin/settings/globalConfig",
                moduleId: "viewmodels/manage/globalConfig/globalConfigPeriodicExport",
                title: "Periodic export",
                tooltip: "",
                nav: true,
                css: 'icon-periodic-export',
                dynamicHash: appUrl.forGlobalConfigPeriodicExport
            }),
            new leafMenuItem({
                route: "admin/settings/globalConfigDatabaseSettings",
                moduleId: "viewmodels/manage/globalConfig/globalConfigDatabaseSettings",
                title: "Cluster-wide database settings",
                tooltip: "Global cluster-wide database settings",
                nav: true,
                css: 'icon-cluster-wide-database-settings',
                dynamicHash: appUrl.forGlobalConfigDatabaseSettings
            }),
            new leafMenuItem({
                route: "admin/settings/globalConfigReplication",
                moduleId: "viewmodels/manage/globalConfig/globalConfigReplications",
                title: "Replication",
                tooltip: "Global replication settings",
                nav: true,
                css: 'icon-repplication',
                dynamicHash: appUrl.forGlobalConfigReplication
            }),
            new leafMenuItem({
                route: "admin/settings/globalConfigSqlReplication",
                moduleId: "viewmodels/manage/globalConfig/globalConfigSqlReplication",
                title: "SQL Replication",
                tooltip: "Global SQL replication settings",
                nav: true,
                css: 'icon-sql-replication',
                dynamicHash: appUrl.forGlobalConfigSqlReplication
            }),
            new leafMenuItem({
                route: "admin/settings/globalConfigQuotas",
                moduleId: "viewmodels/manage/globalConfig/globalConfigQuotas",
                title: "Quotas",
                tooltip: "Global quotas settings",
                nav: true,
                css: 'icon-quotas',
                dynamicHash: appUrl.forGlobalConfigQuotas
            }),
            new leafMenuItem({
                route: "admin/settings/globalConfigCustomFunctions",
                moduleId: "viewmodels/manage/globalConfig/globalConfigCustomFunctions",
                title: "Custom functions",
                tooltip: "Global custom functions settings",
                nav: true,
                css: 'icon-custom-functions',
                dynamicHash: appUrl.forGlobalConfigCustomFunctions
            }),
            new leafMenuItem({
                route: "admin/settings/globalConfigVersioning",
                moduleId: "viewmodels/manage/globalConfig/globalConfigVersioning",
                title: "Versioning",
                tooltip: "Global versioning settings",
                nav: true,
                css: 'icon-versioning',
                dynamicHash: appUrl.forGlobalConfigVersioning
            })     
        ]),
        new leafMenuItem({
            route: ['admin/settings', 'admin/settings/apiKeys'],
            moduleId: 'viewmodels/manage/apiKeys',
            title: 'API Keys',
            nav: true,
            css: 'icon-api-keys',
            dynamicHash: appUrl.forApiKeys,
            enabled: canReadOrWrite
        }),
        new leafMenuItem({
            route: 'admin/settings/cluster',
            moduleId: "viewmodels/manage/cluster",
            title: "Cluster",
            nav: true,
            css: 'icon-cluster',
            dynamicHash: appUrl.forCluster,
            enabled: canReadOrWrite
        }),
        new leafMenuItem({
            route: "admin/settings/serverSmuggling",
            moduleId: "viewmodels/manage/serverSmuggling",
            title: "Server Smuggling",
            nav: true,
            css: 'icon-server-smugling',
            dynamicHash: appUrl.forServerSmugging,
            enabled: accessHelper.isGlobalAdmin
        }),
        new leafMenuItem({
            route: 'admin/settings/backup',
            moduleId: 'viewmodels/manage/backup',
            title: 'Backup',
            nav: true,
            css: 'icon-backup',
            dynamicHash: appUrl.forBackup,
            enabled: accessHelper.isGlobalAdmin
        }),
        new leafMenuItem({
            route: 'admin/settings/compact',
            moduleId: 'viewmodels/manage/compact',
            title: 'Compact',
            nav: true,
            css: 'icon-compact',
            dynamicHash: appUrl.forCompact,
            enabled: accessHelper.isGlobalAdmin
        }),
        new leafMenuItem({
            route: 'admin/settings/restore',
            moduleId: 'viewmodels/manage/restore',
            title: 'Restore',
            nav: true,
            css: 'icon-restore',
            dynamicHash: appUrl.forRestore,
            enabled: accessHelper.isGlobalAdmin
        }),
        new leafMenuItem({
            route: 'admin/settings/adminLogs',
            moduleId: 'viewmodels/manage/adminLogs',
            title: 'Admin Logs',
            nav: true,
            css: 'icon-admin-logs',
            dynamicHash: appUrl.forAdminLogs,
            enabled: accessHelper.isGlobalAdmin
        }),
        new leafMenuItem({
            route: 'admin/settings/topology',
            moduleId: 'viewmodels/manage/topology',
            title: 'Server Topology',
            nav: true,
            css: 'icon-server-topology',
            dynamicHash: appUrl.forServerTopology,
            enabled: accessHelper.isGlobalAdmin
        }),
        new leafMenuItem({
            route: 'admin/settings/trafficWatch',
            moduleId: 'viewmodels/manage/trafficWatch',
            title: 'Traffic Watch',
            nav: true,
            css: 'icon-trafic-watch',
            dynamicHash: appUrl.forTrafficWatch,
            enabled: accessHelper.isGlobalAdmin
        }),
        new leafMenuItem({
            route: 'admin/settings/licenseInformation',
            moduleId: 'viewmodels/manage/licenseInformation',
            title: 'License Information',
            nav: true,
            css: 'icon-license-information',
            dynamicHash: appUrl.forLicenseInformation,
            enabled: canReadOrWrite
        }),
        new leafMenuItem({
            route: 'admin/settings/debugInfo',
            moduleId: 'viewmodels/manage/infoPackage',
            title: 'Gather Debug Info',
            nav: true,
            css: 'icon-gather-debug-information',
            dynamicHash: appUrl.forDebugInfo,
            enabled: accessHelper.isGlobalAdmin
        }),
        new leafMenuItem({
            route: 'admin/settings/ioTest',
            moduleId: 'viewmodels/manage/ioTest',
            title: 'IO Test',
            nav: true,
            css: 'icon-io-test',
            dynamicHash: appUrl.forIoTest,
            enabled: accessHelper.isGlobalAdmin
        }),
        new leafMenuItem({
            route: 'admin/settings/diskIoViewer',
            moduleId: 'viewmodels/manage/diskIoViewer',
            title: 'Disk IO Viewer',
            nav: true,
            css: 'icon-disk-io-viewer',
            dynamicHash: appUrl.forDiskIoViewer,
            enabled: canReadOrWrite
        }),
        new leafMenuItem({
            route: 'admin/settings/console',
            moduleId: "viewmodels/manage/console",
            title: "Administrator JS Console",
            nav: true,
            css: 'icon-administrator-js-console',
            dynamicHash: appUrl.forAdminJsConsole,
            enabled: accessHelper.isGlobalAdmin
        }),
        new leafMenuItem({
            route: 'admin/settings/studioConfig',
            moduleId: 'viewmodels/manage/studioConfig',
            title: 'Studio Config',
            nav: true,
            css: 'icon-studio-config',
            dynamicHash: appUrl.forStudioConfig,
            enabled: canReadOrWrite
        }),
        new leafMenuItem({
            route: 'admin/settings/hotSpare',
            moduleId: 'viewmodels/manage/hotSpare',
            title: 'Hot Spare',
            nav: true,
            css: 'icon-hot-spare',
            dynamicHash: appUrl.forHotSpare,
            enabled: accessHelper.isGlobalAdmin
        })
    ];


    return new leafMenuItem({
        route: 'admin/settings/manage',
        moduleId: 'viewmodels/manage/manageServer',
        title: 'Manage server',
        nav: true,
        css: 'icon-settings',
        dynamicHash: appUrl.forTempManageServer
        
    });
    //TODO: return new intermediateMenuItem('Manage server', items, 'icon-manage-server');
}


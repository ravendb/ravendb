import appUrl = require("common/appUrl");
import intermediateMenuItem = require("common/shell/menu/intermediateMenuItem");
import leafMenuItem = require("common/shell/menu/leafMenuItem");
import separatorMenuItem = require("common/shell/menu/separatorMenuItem");
import accessManager = require("common/shell/accessManager");

export = getManageServerMenuItem;

function getManageServerMenuItem() {

    const access = accessManager.default.manageServerMenu;
    const accessMainMenu = accessManager.default.mainMenu;
        
    const items: menuItem[] = [
        new leafMenuItem({
            route: 'admin/settings/cluster',
            moduleId: "viewmodels/manage/cluster",
            title: "Cluster",
            nav: true,
            css: 'icon-cluster',
            dynamicHash: appUrl.forCluster,
            enabled: access.enableClusterMenuItem
        }),
        new leafMenuItem({
            route: 'admin/settings/addClusterNode',
            moduleId: "viewmodels/manage/addClusterNode",
            title: "Add Cluster Node",
            nav: false,
            dynamicHash: appUrl.forAddClusterNode,
            enabled: accessManager.default.clusterAdminOrClusterNode,
            itemRouteToHighlight: 'admin/settings/cluster'
        }),           
        new leafMenuItem({
            route: 'admin/settings/clientConfiguration',
            moduleId: 'viewmodels/manage/clientConfiguration',
            title: 'Client Configuration',
            nav: true,
            css: 'icon-client-configuration',
            dynamicHash: appUrl.forGlobalClientConfiguration,
            enabled: access.enableClientConfigurationMenuItem
        }),
        new leafMenuItem({
            route: 'admin/settings/studioConfiguration',
            moduleId: 'viewmodels/manage/studioConfiguration',
            title: 'Studio Configuration',
            nav: true,
            css: 'icon-studio-configuration',
            dynamicHash: appUrl.forGlobalStudioConfiguration,
            enabled: access.enableStudioConfigurationMenuItem
        }),
        new leafMenuItem({
            route: 'admin/settings/adminJsConsole',
            moduleId: "viewmodels/manage/adminJsConsole",
            title: "Admin JS Console",
            nav: access.showAdminJSConsoleMenuItem,
            css: 'icon-administrator-js-console',
            dynamicHash: appUrl.forAdminJsConsole,
            enabled: access.enableAdminJSConsoleMenuItem
        }),
        new leafMenuItem({
            route: 'admin/settings/certificates',
            moduleId: "viewmodels/manage/certificates",
            title: "Certificates",
            nav: true,
            css: 'icon-certificate',
            dynamicHash: appUrl.forCertificates,
            enabled: access.enableCertificatesMenuItem
        }), 
        new separatorMenuItem(),
        new separatorMenuItem('Debug'),
        new leafMenuItem({
            route: 'admin/settings/adminLogs',
            moduleId: 'viewmodels/manage/adminLogs',
            title: 'Admin Logs',
            nav: true,
            css: 'icon-admin-logs',
            dynamicHash: appUrl.forAdminLogs,
            enabled: access.enableAdminLogsMenuItem
        }),
        new leafMenuItem({
            route: 'admin/settings/trafficWatch',
            moduleId: 'viewmodels/manage/trafficWatch',
            title: 'Traffic Watch',
            nav: true,
            css: 'icon-traffic-watch',
            dynamicHash: appUrl.forTrafficWatch,
            enabled: access.enableTrafficWatchMenuItem
        }),
        new leafMenuItem({
            route: 'admin/settings/debugInfo',
            moduleId: 'viewmodels/manage/infoPackage',
            title: 'Gather Debug Info',
            nav: true,
            css: 'icon-gather-debug-information',
            dynamicHash: appUrl.forDebugInfo,
            enabled: access.enableGatherDebugInfoMenuItem
        }),
        new leafMenuItem({
            route: 'admin/settings/captureStackTraces',
            moduleId: 'viewmodels/manage/captureStackTraces',
            title: 'Stack Traces',
            nav: true,
            css: 'icon-parallel-stacks', 
            dynamicHash: appUrl.forCaptureStackTraces,
            enabled: access.enableCaptureStackTraces,
        }),
        new leafMenuItem({
            route: 'admin/settings/runningQueries',
            moduleId: 'viewmodels/manage/runningQueries',
            title: 'Running Queries',
            nav: true,
            css: 'icon-manage-server-running-queries',
            dynamicHash: appUrl.forRunningQueries
        }),
        new leafMenuItem({
            route: 'admin/settings/debug/advanced*details',
            moduleId: 'viewmodels/manage/debugAdvancedParent',
            title: 'Advanced',
            nav: true,
            css: 'icon-debug-advanced',
            hash: appUrl.forDebugAdvancedThreadsRuntime(),
            enabled: access.enableAdvancedMenuItem
        }),
    ];

    return new intermediateMenuItem('Manage Server', items, 'icon-manage-server', null, accessMainMenu.showManageServerMenuItem);
}


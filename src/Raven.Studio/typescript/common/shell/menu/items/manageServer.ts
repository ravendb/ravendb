import appUrl = require("common/appUrl");
import settingsAccessAuthorizer = require("common/settingsAccessAuthorizer");
import accessHelper = require("viewmodels/shell/accessHelper");
import intermediateMenuItem = require("common/shell/menu/intermediateMenuItem");
import leafMenuItem = require("common/shell/menu/leafMenuItem");
import separatorMenuItem = require("common/shell/menu/separatorMenuItem");

export = getManageServerMenuItem;

function getManageServerMenuItem() {
    let canReadOrWrite = settingsAccessAuthorizer.canReadOrWrite;
    const items: menuItem[] = [
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
            route: 'admin/settings/addClusterNode',
            moduleId: "viewmodels/manage/addClusterNode",
            title: "Add Cluster Node",
            nav: false,
            dynamicHash: appUrl.forAddClusterNode,
            enabled: canReadOrWrite,
            itemRouteToHighlight: 'admin/settings/cluster'
        }),           
        new leafMenuItem({
            route: 'admin/settings/clientConfiguration',
            moduleId: 'viewmodels/manage/clientConfiguration',
            title: 'Client Configuration',
            nav: true,
            css: 'icon-client-configuration',
            dynamicHash: appUrl.forGlobalClientConfiguration,
            enabled: accessHelper.isGlobalAdmin
        }),
        new leafMenuItem({
            route: 'admin/settings/adminJsConsole',
            moduleId: "viewmodels/manage/adminJsConsole",
            title: "Admin JS Console",
            nav: true,
            css: 'icon-administrator-js-console',
            dynamicHash: appUrl.forAdminJsConsole,
            enabled: accessHelper.isGlobalAdmin
        }),
        new leafMenuItem({
            route: 'admin/settings/certificates',
            moduleId: "viewmodels/manage/certificates",
            title: "Certificates",
            nav: true,
            css: 'icon-certificate',
            dynamicHash: appUrl.forCertificates,
            enabled: accessHelper.isGlobalAdmin
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
            enabled: accessHelper.isGlobalAdmin
        }),
        new leafMenuItem({
            route: 'admin/settings/trafficWatch',
            moduleId: 'viewmodels/manage/trafficWatch',
            title: 'Traffic Watch',
            nav: true,
            css: 'icon-traffic-watch',
            dynamicHash: appUrl.forTrafficWatch,
            enabled: accessHelper.isGlobalAdmin
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
            route: 'admin/settings/debug/advanced*details',
            moduleId: 'viewmodels/manage/debugAdvancedParent',
            title: 'Advanced',
            nav: true,
            css: 'icon-debug-advanced',
            hash: appUrl.forDebugAdvancedThreadsRuntime(),
            enabled: accessHelper.isGlobalAdmin
        }),
    ];

    return new intermediateMenuItem('Manage Server', items, 'icon-manage-server');
}


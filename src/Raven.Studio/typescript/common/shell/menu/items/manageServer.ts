import appUrl = require("common/appUrl");
import settingsAccessAuthorizer = require("common/settingsAccessAuthorizer");
import accessHelper = require("viewmodels/shell/accessHelper");
import intermediateMenuItem = require("common/shell/menu/intermediateMenuItem");
import leafMenuItem = require("common/shell/menu/leafMenuItem");

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
            route: 'admin/settings/clusterObserverLog',
            moduleId: "viewmodels/manage/clusterObserverLog",
            title: "Cluster Observer Log",
            nav: true,
            css: 'icon-cluster',
            dynamicHash: appUrl.forClusterObserverLog,
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
            route: 'admin/settings/debugInfo',
            moduleId: 'viewmodels/manage/infoPackage',
            title: 'Gather Debug Info',
            nav: true,
            css: 'icon-gather-debug-information',
            dynamicHash: appUrl.forDebugInfo,
            enabled: accessHelper.isGlobalAdmin
        }),
        new leafMenuItem({
            route: 'admin/settings/adminJsConsole',
            moduleId: "viewmodels/manage/adminJsConsole",
            title: "Administrator JS Console",
            nav: true,
            css: 'icon-administrator-js-console',
            dynamicHash: appUrl.forAdminJsConsole,
            enabled: accessHelper.isGlobalAdmin
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
            route: 'admin/settings/certificates',
            moduleId: "viewmodels/manage/certificates",
            title: "Certificates",
            nav: true,
            css: 'icon-certificate',
            dynamicHash: appUrl.forCertificates,
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
            route: 'admin/settings/trafficWatch',
            moduleId: 'viewmodels/manage/trafficWatch',
            title: 'Traffic Watch',
            nav: true,
            css: 'icon-trafic-watch',
            dynamicHash: appUrl.forTrafficWatch,
            enabled: accessHelper.isGlobalAdmin
        }),
        /* TODO
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
            route: 'admin/settings/topology',
            moduleId: 'viewmodels/manage/topology',
            title: 'Server Topology',
            nav: true,
            css: 'icon-server-topology',
            dynamicHash: appUrl.forServerTopology,
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
        }),*/
        /*
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
            route: 'admin/settings/studioConfig',
            moduleId: 'viewmodels/manage/studioConfig',
            title: 'Studio Config',
            nav: true,
            css: 'icon-studio-config',
            dynamicHash: appUrl.forStudioConfig,
            enabled: canReadOrWrite
        }),*/
    ];

    return new intermediateMenuItem('Manage server', items, 'icon-manage-server');
}


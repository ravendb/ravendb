import appUrl = require("common/appUrl");
import { bridgeToReact } from "common/reactUtils";
import intermediateMenuItem = require("common/shell/menu/intermediateMenuItem");
import leafMenuItem = require("common/shell/menu/leafMenuItem");
import separatorMenuItem = require("common/shell/menu/separatorMenuItem");
import AdminJsConsole from "components/pages/resources/manageServer/adminJsConsole/AdminJsConsole";
import ClientGlobalConfiguration from "components/pages/resources/manageServer/clientConfiguration/ClientGlobalConfiguration";
import StudioGlobalConfiguration from "components/pages/resources/manageServer/studioConfiguration/StudioGlobalConfiguration";
import GatherDebugInfo from "components/pages/resources/manageServer/gatherDebugInfo/GatherDebugInfo";
import ServerWideCustomAnalyzers from "components/pages/resources/manageServer/serverWideAnalyzers/ServerWideCustomAnalyzers";
import ServerWideCustomSorters from "components/pages/resources/manageServer/serverWideSorters/ServerWideCustomSorters";

export = getManageServerMenuItem;

function getManageServerMenuItem() {
    const items: menuItem[] = [
        new leafMenuItem({
            route: 'admin/settings/cluster',
            moduleId: require("viewmodels/manage/cluster"),
            title: "Cluster",
            nav: true,
            css: 'icon-cluster',
            dynamicHash: appUrl.forCluster,
            search: {
                alternativeTitles: ["Cluster Topology"],
                innerActions: [
                    { name: "Delete Node from Cluster", alternativeNames: ["Remove Node from Cluster"] },
                    { name: "Reassign cores" },
                    { name: "Force timeout" },
                ],
            },
        }),
        new leafMenuItem({
            route: 'admin/settings/addClusterNode',
            moduleId: require("viewmodels/manage/addClusterNode"),
            title: "Add Cluster Node",
            nav: false,
            css: "icon-plus",
            dynamicHash: appUrl.forAddClusterNode,
            itemRouteToHighlight: 'admin/settings/cluster',
            search: {
                overrideTitle: "Add New Cluster Node",
                alternativeTitles: ["Create Cluster Node"],
            }
        }),
        new leafMenuItem({
            route: 'admin/settings/clientConfiguration',
            moduleId: bridgeToReact(ClientGlobalConfiguration, "nonShardedView"),
            title: 'Client Configuration',
            nav: true,
            css: 'icon-client-configuration',
            dynamicHash: appUrl.forGlobalClientConfiguration,
            requiredAccess: "Operator",
            search: {
                innerActions: [
                    { name: "Identity parts separator" },
                    { name: "Maximum number of requests per session" },
                    { name: "Load Balance Behavior" },
                    { name: "Seed" },
                    { name: "Read Balance Behavior" },
                ],
            },
        }),
        new leafMenuItem({
            route: 'admin/settings/studioConfiguration',
            moduleId: bridgeToReact(StudioGlobalConfiguration, "nonShardedView"),
            title: 'Studio Configuration',
            nav: true,
            css: 'icon-studio-configuration',
            dynamicHash: appUrl.forGlobalStudioConfiguration,
            requiredAccess: "Operator",
            search: {
                innerActions: [
                    { name: "Server Environment" },
                    { name: "Default Replication Factor" },
                    { name: "Collapse documents when opening" },
                    { name: "Help improve the Studio by gathering anonymous usage statistics" },
                ],
            },
        }),
        new leafMenuItem({
            route: 'admin/settings/serverSettings',
            moduleId: require("viewmodels/manage/serverSettings"),
            title: 'Server Settings',
            nav: true,
            css: 'icon-server-settings',
            dynamicHash: appUrl.forServerSettings,
            requiredAccess: "ClusterAdmin"
        }),
        new leafMenuItem({
            route: 'admin/settings/adminJsConsole',
            moduleId: bridgeToReact(AdminJsConsole, "nonShardedView"),
            title: "Admin JS Console",
            nav: true,
            css: 'icon-administrator-js-console',
            dynamicHash: appUrl.forAdminJsConsole,
            requiredAccess: "ClusterAdmin"
        }),
        new leafMenuItem({
            route: 'admin/settings/certificates',
            moduleId: require("viewmodels/manage/certificates"),
            title: "Certificates",
            nav: true,
            css: 'icon-certificate',
            dynamicHash: appUrl.forCertificates,
            requiredAccess: "Operator"
        }),
        new leafMenuItem({
            route: 'admin/settings/serverWideTasks',
            moduleId: require("viewmodels/manage/serverWideTasks"),
            title: "Server-Wide Tasks",
            nav: true,
            css: 'icon-server-wide-tasks',
            dynamicHash: appUrl.forServerWideTasks,
            requiredAccess: "ClusterAdmin",
            search: {
                innerActions: [
                    { name: "Server-Wide External Replication" },
                    { name: "Server-Wide Periodic Backup" },
                ],
            },
        }),
        new leafMenuItem({
            route: 'admin/settings/editServerWideBackup',
            moduleId: require("viewmodels/manage/editServerWideBackup"),
            title: "Edit Server-Wide Backup Task",
            nav: false,
            css: "icon-plus",
            dynamicHash: appUrl.forEditServerWideBackup,
            itemRouteToHighlight: 'admin/settings/serverWideTasks',
            requiredAccess: "ClusterAdmin",
            search: {
                overrideTitle: "Add New Server-Wide Backup Task",
                alternativeTitles: ["Create Server-Wide Backup Task"],
            }
        }),
        new leafMenuItem({
            route: 'admin/settings/editServerWideExternalReplication',
            moduleId: require("viewmodels/manage/editServerWideExternalReplication"),
            title: "Edit Server-Wide External Replication Task",
            nav: false,
            css: "icon-plus",
            dynamicHash: appUrl.forEditServerWideExternalReplication,
            itemRouteToHighlight: 'admin/settings/serverWideTasks',
            requiredAccess: "ClusterAdmin",
            search: {
                overrideTitle: "Add New Server-Wide External Replication Task",
                alternativeTitles: ["Create Server-Wide External Replication Task"],
            }
        }),
        new leafMenuItem({
            route: 'admin/settings/serverWideCustomAnalyzers',
            moduleId: bridgeToReact(ServerWideCustomAnalyzers, "nonShardedView"),
            title: "Server-Wide Analyzers",
            nav: true,
            css: 'icon-server-wide-custom-analyzers',
            dynamicHash: appUrl.forServerWideCustomAnalyzers,
            requiredAccess: "Operator",
            search: {
                innerActions: [
                    { name: "Add New Server-Wide Custom Analyzer", alternativeNames: ["Create Server-Wide Custom Analyzer"] },
                    { name: "Edit Server-Wide Custom Analyzer" },
                    { name: "DeleteServer-Wide Custom Analyzer", alternativeNames: ["Remove Server-Wide Custom Analyzer"] },
                ],
            },
        }),
        new leafMenuItem({
            route: 'admin/settings/serverWideCustomSorters',
            moduleId: bridgeToReact(ServerWideCustomSorters, "nonShardedView"),
            title: "Server-Wide Sorters",
            nav: true,
            css: 'icon-server-wide-custom-sorters',
            dynamicHash: appUrl.forServerWideCustomSorters,
            requiredAccess: "Operator",
            search: {
                innerActions: [
                    { name: "Add New Server-Wide Custom Sorter", alternativeNames: ["Create Server-Wide Custom Sorter"] },
                    { name: "Edit New Server-Wide Custom Sorter" },
                    { name: "Delete New Server-Wide Custom Sorter", alternativeNames: ["Remove New Server-Wide Custom Sorter"] },
                ],
            },
        }),
        new separatorMenuItem(),
        new separatorMenuItem('Debug'),
        new leafMenuItem({
            route: 'admin/settings/adminLogs',
            moduleId: require("viewmodels/manage/adminLogs"),
            title: 'Admin Logs',
            nav: true,
            css: 'icon-admin-logs',
            dynamicHash: appUrl.forAdminLogs,
            requiredAccess: "Operator",
            search: {
                innerActions: [
                    { name: "Export logs" },
                    { name: "Download logs" },
                ],
            },
        }),
        new leafMenuItem({
            route: 'admin/settings/trafficWatch',
            moduleId: require("viewmodels/manage/trafficWatch"),
            title: 'Traffic Watch',
            nav: true,
            css: 'icon-traffic-watch',
            dynamicHash: appUrl.forTrafficWatch,
            requiredAccess: "Operator",
            search: {
                innerActions: [
                    { name: "Export Traffic Logs" }
                ],
            },
        }),
        new leafMenuItem({
            route: 'admin/settings/debugInfo',
            moduleId: bridgeToReact(GatherDebugInfo, "nonShardedView"),
            title: 'Gather Debug Info',
            nav: true,
            css: 'icon-gather-debug-information',
            dynamicHash: appUrl.forDebugInfo,
            requiredAccess: "Operator",
            search: {
                alternativeTitles: ["Create Debug Package"],
                innerActions: [
                    { name: "Gather Debug Info" },
                    { name: "Download Debug Package" },
                ],
            }
        }),
        new leafMenuItem({
            route: 'admin/settings/storageReport',
            moduleId: require('viewmodels/manage/storageReport'),
            title: 'Storage Report',
            tooltip: "Storage Report",
            nav: true,
            css: 'icon-system-storage',
            dynamicHash: appUrl.forSystemStorageReport,
            requiredAccess: "Operator"
        }),
        new leafMenuItem({
            route: 'admin/settings/ioStats',
            moduleId: require('viewmodels/manage/serverWideIoStats'),
            title: 'IO Stats',
            tooltip: "Displays IO metrics status",
            nav: true,
            css: 'icon-manage-server-io-test',
            dynamicHash: appUrl.forSystemIoStats,
            requiredAccess: "Operator",
            search: {
                innerActions: [
                    { name: "Export IO stats" },
                    { name: "Import IO stats" },
                ],
            },
        }),
        new leafMenuItem({
            route: 'admin/settings/captureStackTraces',
            moduleId: require('viewmodels/manage/captureStackTraces'),
            title: 'Stack Traces',
            nav: true,
            css: 'icon-stack-traces', 
            dynamicHash: appUrl.forCaptureStackTraces,
            requiredAccess: "Operator",
            search: {
                innerActions: [
                    { name: "Capture Stack Trace" },
                    { name: "Export Stack Traces" },
                    { name: "Import Stack Traces" },
                ]
            }
        }),
        new leafMenuItem({
            route: 'admin/settings/runningQueries',
            moduleId: require('viewmodels/manage/runningQueries'),
            title: 'Running Queries',
            nav: true,
            css: 'icon-manage-server-running-queries',
            dynamicHash: appUrl.forRunningQueries
        }),
        new leafMenuItem({
            route: 'admin/settings/debug/advanced*details',
            moduleId: require('viewmodels/manage/debugAdvancedParent'),
            title: 'Advanced',
            nav: true,
            css: 'icon-debug-advanced',
            dynamicHash: appUrl.forDebugAdvancedThreadsRuntime,
            requiredAccess: "Operator",
        }),
        new leafMenuItem({
            route: 'admin/settings/debug/advanced/threadsRuntime',
            moduleId: require('viewmodels/manage/debugAdvancedThreadsRuntime'),
            title: 'Advanced',
            nav: false,
            css: 'icon-debug-advanced',
            dynamicHash: appUrl.forDebugAdvancedThreadsRuntime,
            itemRouteToHighlight: 'admin/settings/debug/advanced*details',
            requiredAccess: "Operator",
            search: {
                overrideTitle: "Threads Runtime Info",
            },
        }),
        new leafMenuItem({
            route: 'admin/settings/debug/advanced/memoryMappedFiles',
            moduleId: require('viewmodels/manage/debugAdvancedMemoryMappedFiles'),
            title: "Advanced",
            nav: false,
            css: 'icon-debug-advanced',
            dynamicHash: appUrl.forDebugAdvancedMemoryMappedFiles,
            itemRouteToHighlight: 'admin/settings/debug/advanced*details',
            requiredAccess: "Operator",
            search: {
                overrideTitle: 'Memory Mapped Files',
            }
        }),
        new leafMenuItem({
            route: 'admin/settings/debug/advanced/observerLog',
            moduleId: require('viewmodels/manage/debugAdvancedObserverLog'),
            title: 'Advanced',
            nav: false,
            css: 'icon-debug-advanced',
            dynamicHash: appUrl.forDebugAdvancedObserverLog,
            itemRouteToHighlight: 'admin/settings/debug/advanced*details',
            requiredAccess: "Operator",
            search: {
                overrideTitle: 'Cluster Observer Log',
            }
        }),
        new leafMenuItem({
            route: 'admin/settings/debug/advanced/recordTransactionCommands',
            moduleId: require('viewmodels/manage/debugAdvancedRecordTransactionCommands'),
            title: 'Advanced',
            nav: false,
            css: 'icon-debug-advanced',
            dynamicHash: appUrl.forDebugAdvancedRecordTransactionCommands,
            itemRouteToHighlight: 'admin/settings/debug/advanced*details',
            requiredAccess: "Operator",
            search: {
                overrideTitle: 'Record Transaction Commands',
            }
        }),
        new leafMenuItem({
            route: 'admin/settings/debug/advanced/replayTransactionCommands',
            moduleId: require('viewmodels/manage/debugAdvancedReplayTransactionCommands'),
            title: 'Advanced',
            nav: false,
            css: 'icon-debug-advanced',
            dynamicHash: appUrl.forDebugAdvancedReplayTransactionCommands,
            itemRouteToHighlight: 'admin/settings/debug/advanced*details',
            requiredAccess: "Operator",
            search: {
                overrideTitle: 'Replay Transaction Commands',
            }
        }),
    ];

    return new intermediateMenuItem('Manage Server', items, 'icon-manage-server', null);
}

import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");
import intermediateMenuItem = require("common/shell/menu/intermediateMenuItem");
import leafMenuItem = require("common/shell/menu/leafMenuItem");

export = getStatsMenuItem;

function getStatsMenuItem(appUrls: computedAppUrls) {
    let activeDatabase = activeDatabaseTracker.default.database;
    var items: menuItem[] = [
        new leafMenuItem({
            route: 'databases/status/storage/report',
            moduleId: 'viewmodels/database/status/storageReport',
            title: 'Storage Report',
            tooltip: "TODO", //TODO:
            nav: true,
            css: 'icon-storage',
            dynamicHash: appUrls.statusStorageReport
        }),
        /* TODO
        new intermediateMenuItem('Storage', [
            new leafMenuItem({
                route: 'databases/status/storage',
                moduleId: 'viewmodels/database/status/storage/statusStorageOnDisk',
                title: 'On disk',
                tooltip: "Shows disk usage for active database",
                nav: accessHelper.isGlobalAdmin(),
                dynamicHash: appUrls.statusStorageOnDisk
            }),
            new leafMenuItem({
                route: 'databases/status/storage/storageBreakdown',
                moduleId: 'viewmodels/database/status/storage/statusStorageBreakdown',
                title: 'Internal storage Breakdown',
                tooltip: "Shows detailed information about internal storage breakdown",
                nav: accessHelper.isGlobalAdmin(),
                dynamicHash: appUrls.statusStorageBreakdown
            }),
            new leafMenuItem({
                route: 'databases/status/storage/collections',
                moduleId: 'viewmodels/database/status/storage/statusStorageCollections',
                title: 'Collections storage',
                tooltip: "Shows document counts (VERY SLOW)",
                nav: true,
                dynamicHash: appUrls.statusStorageCollections
            })
        ], 'icon-plus'),*/
        /* TODO
        new intermediateMenuItem('Debug', [
            new leafMenuItem({
                route: 'databases/status/debug',
                moduleId: 'viewmodels/database/status/debug/statusDebugChanges',
                title: 'Changes',
                tooltip: 'Shows information about active changes API subscriptions',
                nav: true,
                dynamicHash: appUrls.statusDebugChanges
            }),
            new leafMenuItem({
                route: 'databases/status/debug/metrics',
                moduleId: 'viewmodels/database/status/debug/statusDebugMetrics',
                title: 'Metrics',
                tooltip: "Shows database metrics",
                nav: true,
                dynamicHash: appUrls.statusDebugMetrics
            }),
            new leafMenuItem({
                route: 'databases/status/debug/config',
                moduleId: 'viewmodels/database/status/debug/statusDebugConfig',
                title: 'Config',
                tooltip: "Displays server configuration",
                nav: true,
                dynamicHash: appUrls.statusDebugConfig
            }),
            new leafMenuItem({
                route: 'databases/status/debug/docrefs',
                moduleId: 'viewmodels/database/status/debug/statusDebugDocrefs',
                title: 'Doc refs',
                tooltip: "Allows to find documents referenced by given document id",
                nav: true,
                dynamicHash: appUrls.statusDebugDocrefs
            }),
            new leafMenuItem({
                route: 'databases/status/debug/currentlyIndexing',
                moduleId: 'viewmodels/database/status/debug/statusDebugCurrentlyIndexing',
                title: 'Currently indexing',
                tooltip: "Displays currently performed indexing work",
                nav: true,
                dynamicHash: appUrls.statusDebugCurrentlyIndexing
            }),
            new leafMenuItem({
                route: 'databases/status/debug/queries',
                moduleId: 'viewmodels/database/status/debug/statusDebugQueries',
                title: 'Queries',
                tooltip: "Displays currently running queries",
                nav: true,
                dynamicHash: appUrls.statusDebugQueries
            }),
            new leafMenuItem({
                route: 'databases/status/debug/tasks',
                moduleId: 'viewmodels/database/status/debug/statusDebugTasks',
                title: 'Tasks',
                tooltip: "Displays currently running index tasks",
                nav: true,
                dynamicHash: appUrls.statusDebugTasks
            }),
            new leafMenuItem({
                route: 'databases/status/debug/routes',
                moduleId: 'viewmodels/database/status/debug/statusDebugRoutes',
                title: 'Routes',
                tooltip: "Displays all available routes",
                nav: accessHelper.isGlobalAdmin(),
                dynamicHash: appUrls.statusDebugRoutes
            }),
            new leafMenuItem({
                route: 'databases/status/debug/sqlReplication',
                moduleId: 'viewmodels/database/status/debug/statusDebugSqlReplication',
                title: 'SQL Replication',
                tooltip: "Shows information about SQL replication",
                nav: activeDatabase() && activeDatabase().isBundleActive("SqlReplication"),
                dynamicHash: appUrls.statusDebugSqlReplication
            }),
            new leafMenuItem({
                route: 'databases/status/debug/indexFields',
                moduleId: 'viewmodels/database/status/debug/statusDebugIndexFields',
                title: 'Index fields',
                tooltip: "Shows names of indexed fields based on entered index definition",
                nav: true,
                dynamicHash: appUrls.statusDebugIndexFields
            }),
            new leafMenuItem({
                route: 'databases/status/debug/identities',
                moduleId: 'viewmodels/database/status/debug/statusDebugIdentities',
                title: 'Identities',
                tooltip: "Shows identities values for collections",
                nav: true,
                dynamicHash: appUrls.statusDebugIdentities
            }),
            new leafMenuItem({
                route: 'databases/status/debug/websocket',
                moduleId: 'viewmodels/database/status/debug/statusDebugWebSocket',
                title: 'Web Socket',
                tooltip: "Allows to debug websockets connection",
                nav: true,
                dynamicHash: appUrls.statusDebugWebSocket
            }),
            new leafMenuItem({
                route: 'databases/status/debug/explainReplication',
                moduleId: 'viewmodels/database/status/debug/statusDebugExplainReplication',
                title: 'Explain replication',
                tooltip: "Shows information about replication of given document to given replication destination",
                nav: activeDatabase() && activeDatabase().isBundleActive("Replication"),
                dynamicHash: appUrls.statusDebugExplainReplication
            })
        ], 'icon-plus'),*/
        new leafMenuItem({
            route: 'databases/status/ioStats',
            moduleId: 'viewmodels/database/status/ioStats',
            title: 'IO Stats',
            tooltip: "Displays IO metrics statatus",
            nav: true,
            css: 'icon-io-test',
            dynamicHash: appUrls.ioStats
        }),
        /* TODO:
        new leafMenuItem({
            route: 'databases/status/requests',
            moduleId: 'viewmodels/database/status/requests/requestsCount',
            title: 'Requests count',
            tooltip: "Displays requests counts over time",
            nav: true,
            dynamicHash: appUrls.requestsCount
        }),
        new leafMenuItem({
            route: 'databases/status/requests/tracing',
            moduleId: 'viewmodels/database/status/requests/requestTracing',
            title: 'Request tracing',
            tooltip: "Displays recent requests with their status and execution times",
            nav: accessHelper.canExposeConfigOverTheWire(),
            dynamicHash: appUrls.requestsTracing
        }),*/
        new leafMenuItem({
            route: 'databases/status',
            moduleId: 'viewmodels/database/status/statistics',
            title: 'Stats',
            nav: true,
            css: 'icon-stats',
            dynamicHash: appUrls.status
        }),

        new leafMenuItem({
            route: 'databases/status/subscriptions',
            moduleId: 'viewmodels/database/status/subscriptions',
            title: 'Subscriptions',
            nav: true,
            css: 'icon-subscriptions',
            dynamicHash: appUrls.subscriptions
        }),
        /* TODO: 
        new leafMenuItem({
            route: 'databases/status/logs',
            moduleId: 'viewmodels/database/status/logs',
            title: 'Logs',
            nav: true,
            dynamicHash: appUrls.logs
        }),*/
        /*TODO
        new leafMenuItem({
            route: 'databases/status/runningTasks',
            moduleId: 'viewmodels/database/status/runningTasks',
            title: 'Running Tasks',
            nav: true,
            dynamicHash: appUrls.runningTasks
        }),*/
        /* TODO
        new leafMenuItem({
            route: 'databases/status/alerts',
            moduleId: 'viewmodels/database/status/alerts',
            title: 'Alerts',
            nav: true,
            dynamicHash: appUrls.alerts
        }),
        ,*/
        new leafMenuItem({
            route: 'databases/status/replicationStats',
            moduleId: 'viewmodels/database/status/replicationStats',
            title: 'Replication Stats',
            nav: true,
            css: 'icon-versioning',
            dynamicHash: appUrls.replicationStats
        }),
        /* TODO
        new leafMenuItem({
            route: 'databases/status/sqlReplicationPerfStats',
            moduleId: 'viewmodels/database/status/sqlReplicationPerfStats',
            title: 'SQL Replication Stats',
            nav: true,
            dynamicHash: appUrls.sqlReplicationPerfStats
        }),*/
        /* TODO:
        new leafMenuItem({
            route: 'databases/status/userInfo',
            moduleId: 'viewmodels/database/status/userInfo',
            title: 'User Info',
            nav: true,
            dynamicHash: appUrls.userInfo
        }),*/
        new leafMenuItem({
            route: 'databases/status/debug*details',
            moduleId: 'viewmodels/database/status/debug/statusDebug',
            title: 'Debug',
            nav: false,
            css: 'icon-debug',
            dynamicHash: appUrls.statusDebug
        }),
        /* TODO
        new leafMenuItem({
            route: 'databases/status/storage*details',
            moduleId: 'viewmodels/database/status/storage/statusStorage',
            title: 'Storage',
            nav: false,
            dynamicHash: appUrls.statusStorageOnDisk
        }),*/
        /* TODO
        new leafMenuItem({
            route: 'databases/status/infoPackage',
            moduleId: 'viewmodels/manage/infoPackage',
            title: 'Gather Debug Info',
            nav: accessHelper.canExposeConfigOverTheWire(),
            dynamicHash: appUrls.infoPackage
        })*/
    ];

    return new intermediateMenuItem("Stats", items, "icon-stats");
}

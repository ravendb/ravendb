import intermediateMenuItem = require("common/shell/menu/intermediateMenuItem");
import leafMenuItem = require("common/shell/menu/leafMenuItem");
import separatorMenuItem = require("common/shell/menu/separatorMenuItem");

export = getSettingsMenuItem;

function getSettingsMenuItem(appUrls: computedAppUrls) {
    var items: menuItem[] = [
        new leafMenuItem({
            route: ['databases/record', 'databases/settings/databaseRecord'],
            moduleId: 'viewmodels/database/settings/databaseRecord',
            title: 'Database Record',
            nav: true,
            css: 'icon-database-settings',
            dynamicHash: appUrls.databaseRecord
        }),
        /* TODO
        new leafMenuItem({
            route: 'databases/settings/quotas',
            moduleId: 'viewmodels/database/settings/quotas',
            title: 'Quotas',
            nav: true,
            css: 'icon-plus',
            dynamicHash: appUrls.quotas
        }),*/
        /* TODO: move to tasks menu
        new leafMenuItem({
            route: 'databases/settings/replication',
            moduleId: 'viewmodels/database/settings/replications',
            title: 'Replication',
            nav: true,
            css: 'icon-replication',
            dynamicHash: appUrls.replications
        }),*/
        /* TODO
        new leafMenuItem({
            route: 'databases/settings/etl',
            moduleId: 'viewmodels/database/settings/etl',
            title: 'ETL',
            nav: true,
            css: 'icon-plus',
            dynamicHash: appUrls.etl
        }),*/
        /* TODO
        new leafMenuItem({
            route: 'databases/settings/sqlReplication',
            moduleId: 'viewmodels/database/settings/sqlReplications',
            title: 'SQL Replication',
            nav: true,
            css: 'icon-sql-replication',
            dynamicHash: appUrls.sqlReplications
        }),*/
        new leafMenuItem({
            route: 'databases/settings/editSqlReplication(/:sqlReplicationName)',
            moduleId: 'viewmodels/database/settings/editSqlReplication',
            title: 'Edit SQL Replication',
            nav: false,
            css: 'icon-sql-replication',
            dynamicHash: appUrls.editSqlReplication
        }),
        new leafMenuItem({
            route: 'databases/settings/sqlReplicationConnectionStringsManagement',
            moduleId: 'viewmodels/database/settings/sqlReplicationConnectionStringsManagement',
            title: 'SQL Replication Connection Strings',
            nav: false,
            css: 'icon-sql-replication-connection-string',
            dynamicHash: appUrls.sqlReplicationsConnections
        }),
        new leafMenuItem({
            route: 'databases/settings/revisions',
            moduleId: 'viewmodels/database/settings/revisions',
            title: 'Document Revisions',
            nav: true,
            css: 'icon-revisions',
            dynamicHash: appUrls.revisions
        }),
        new leafMenuItem({
            route: 'databases/settings/customFunctionsEditor',
            moduleId: 'viewmodels/database/settings/customFunctionsEditor',
            title: 'Custom Functions',
            nav: true,
            css: 'icon-custom-functions',
            dynamicHash: appUrls.customFunctionsEditor
        }),
        new leafMenuItem({
            route: 'databases/manageDatabaseGroup',
            moduleId: 'viewmodels/resources/manageDatabaseGroup',
            title: 'Manage database group',
            nav: false,
            css: 'icon-topology',
            dynamicHash: appUrls.manageDatabaseGroup
        }),
        /*TODO
        new leafMenuItem({
            route: 'databases/settings/databaseStudioConfig',
            moduleId: 'viewmodels/databaseStudioConfig',
            title: 'Studio Config',
            nav: true,
            css: 'icon-studio-config',
            dynamicHash: appUrls.databaseStudioConfig
        })*/

        new separatorMenuItem('Tasks'),
        new leafMenuItem({
            route: 'databases/tasks/ongoingTasks',
            moduleId: 'viewmodels/database/tasks/ongoingTasks',
            title: 'Manage Ongoing Tasks',
            nav: true,
            css: 'icon-manage-ongoing-tasks',
            dynamicHash: appUrls.ongoingTasksUrl
        }),
        new leafMenuItem({
            route: 'databases/tasks/editExternalReplicationTask',
            moduleId: 'viewmodels/database/tasks/editExternalReplicationTask',
            title: 'External Replication Task',
            nav: false,
            dynamicHash: appUrls.editExternalReplicationTaskUrl,
            itemRouteToHighlight: 'databases/tasks/ongoingTasks'
        }),
        new leafMenuItem({
            route: 'databases/tasks/editPeriodicBackupTask',
            moduleId: 'viewmodels/database/tasks/editPeriodicBackupTask',
            title: 'Backup Task',
            nav: false,
            dynamicHash: appUrls.editExternalReplicationTaskUrl,
            itemRouteToHighlight: 'databases/tasks/ongoingTasks'
        }),
        new leafMenuItem({
            route: 'databases/tasks/editSubscriptionTask',
            moduleId: 'viewmodels/database/tasks/editSubscriptionTask',
            title: 'Subscription Task',
            nav: false,
            dynamicHash: appUrls.editSubscriptionTaskUrl,
            itemRouteToHighlight: 'databases/tasks/ongoingTasks'
        }),
        new leafMenuItem({
            route: 'databases/tasks/importDatabase',
            moduleId: 'viewmodels/database/tasks/importDatabase',
            title: 'Import Database',
            nav: true,
            css: 'icon-import-database',
            dynamicHash: appUrls.importDatabaseUrl
        }),
        new leafMenuItem({
            route: 'databases/tasks/exportDatabase',
            moduleId: 'viewmodels/database/tasks/exportDatabase',
            title: 'Export Database',
            nav: true,
            css: 'icon-export-database',
            dynamicHash: appUrls.exportDatabaseUrl
        }),
        new leafMenuItem({
            route: 'databases/tasks/sampleData',
            moduleId: 'viewmodels/database/tasks/createSampleData',
            title: 'Create Sample Data',
            nav: true,
            css: 'icon-create-sample-data',
            dynamicHash: appUrls.sampleDataUrl
        }),
        /* TODO:
        new leafMenuItem({
            route: 'databases/tasks/csvImport',
            moduleId: 'viewmodels/database/tasks/csvImport',
            title: 'CSV Import',
            nav: true,
            css: 'icon-plus',
            dynamicHash: csvImportUrl
        })*/

        new separatorMenuItem('Statistics'),
        new leafMenuItem({
            route: 'databases/status',
            moduleId: 'viewmodels/database/status/statistics',
            title: 'Statistics',
            nav: true,
            css: 'icon-stats',
            dynamicHash: appUrls.status
        }),
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
            css: 'icon-revisions',
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
        })
    ];

    return new intermediateMenuItem('Settings', items, 'icon-settings');
}

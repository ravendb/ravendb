
import database = require("models/resources/database");
import appUrl = require("common/appUrl");

interface GenerateMenuItemOptions {
    activeDatabase: KnockoutObservable<database>;
    canExposeConfigOverTheWire: KnockoutObservable<boolean>;
    isGlobalAdmin: KnockoutObservable<boolean>;
}

export type menuItemType = "separator" | "intermediate" | "leaf";

export interface menuItem {
    type: menuItemType;
}

export class separatorMenuItem implements menuItem {
    title: string;
    type: menuItemType = "separator";

    constructor(title: string) {
        this.title = title;
    }
}

export class intermediateMenuItem implements menuItem {
    title: string;
    children: menuItem[];
    css: string;
    type: menuItemType = "intermediate";

    constructor(title: string, children: menuItem[], css?: string) {
        this.title = title;
        this.children = children;
        this.css = css;
    }
}

export class leafMenuItem implements menuItem {
    title: string;
    tooltip: string;
    nav: boolean;
    route: string | Array<string>;
    moduleId: string;
    hash: KnockoutComputed<string>;
    dynamicHash: KnockoutComputed<string>;
    css: string;
    path: KnockoutComputed<string>;
    type: menuItemType = "leaf";

    constructor({ title, tooltip, route, moduleId, nav, hash, css, dynamicHash }: {
        title: string,
        tooltip?: string,
        route: string | Array<string>,
        moduleId: string,
        nav: boolean,
        hash?: KnockoutComputed<string>,
        dynamicHash?: KnockoutComputed<string>,
        css?: string
    }) {
        this.title = title;
        this.route = route;
        this.moduleId = moduleId;
        this.nav = nav;
        this.hash = hash;
        this.dynamicHash = dynamicHash;
        this.css = css;
        this.path = ko.computed(() => {
            if (this.hash) {
                return this.hash();
            } else if (this.dynamicHash) {
                return this.dynamicHash();
            }

            return null;
        });
    }
}

export function generateMenuItems(opts: GenerateMenuItemOptions) {
    let appUrls = appUrl.forCurrentDatabase();
    let menuItems: menuItem[] = [
        getDocumentsMenuItem(appUrls),
        getIndexesMenuItem(appUrls),
        getQueryMenuItem(appUrls),
        new separatorMenuItem('Manage'),
        getTasksMenuItem(appUrls, opts.activeDatabase),
        getSettingsMenuItem(appUrls),
        getStatsMenuItem(appUrls, opts),
        new separatorMenuItem('Server'),
        getResourcesMenuItem(appUrls, opts),
        getManageServerMenuItem(appUrls, opts),
        new leafMenuItem({
                route: '',
                moduleId: '',
                title: 'About',
                tooltip: "About",
                nav: true,
                css: 'fa fa-question-mark',
                hash: ko.computed(() => 'TODO')
            })
    ];

    return menuItems;
}

function getManageServerMenuItem(appUrls: computedAppUrls,
    { canExposeConfigOverTheWire, isGlobalAdmin, activeDatabase }: GenerateMenuItemOptions) {
    var items: menuItem[] = [

    ];

    return new intermediateMenuItem('Manage server', items, 'icon-settings');
}

function getResourcesMenuItem(appUrls: computedAppUrls, opts: GenerateMenuItemOptions) {
    return new leafMenuItem({
            route: ["", "resources"],
            title: "Resources",
            moduleId: "viewmodels/resources/resources",
            nav: true,
            css: 'icon-resources',
            hash: appUrls.resourcesManagement
        });
}

function getStatsMenuItem(
    appUrls: computedAppUrls,
    { canExposeConfigOverTheWire, isGlobalAdmin, activeDatabase }: GenerateMenuItemOptions) {
    var items: menuItem[] = [
        new intermediateMenuItem('Indexing', [
            new leafMenuItem({
                route: 'databases/status/indexing',
                moduleId: 'viewmodels/database/status/indexing/indexPerformance',
                title: 'Indexing performance',
                tooltip: "Shows details about indexing peformance",
                nav: true,
                dynamicHash: appUrls.indexPerformance
            }),
            new leafMenuItem({
                route: 'databases/status/indexing/stats',
                moduleId: 'viewmodels/database/status/indexing/indexStats',
                title: 'Index stats',
                tooltip: "Show details about indexing in/out counts",
                nav: true,
                dynamicHash: appUrls.indexStats
            }),
            new leafMenuItem({
                route: 'databases/status/indexing/batchSize',
                moduleId: 'viewmodels/database/status/indexing/indexBatchSize',
                title: 'Index batch size',
                tooltip: "Index batch sizes",
                nav: true,
                dynamicHash: appUrls.indexBatchSize
            }),
            new leafMenuItem({
                route: 'databases/status/indexing/prefetches',
                moduleId: 'viewmodels/database/status/indexing/indexPrefetches',
                title: 'Prefetches',
                tooltip: "Prefetches",
                nav: true,
                dynamicHash: appUrls.indexPrefetches
            }) 
        ], 'icon-indexes'),
        new intermediateMenuItem('Storage', [
            new leafMenuItem({
                route: 'databases/status/storage',
                moduleId: 'viewmodels/database/status/storage/statusStorageOnDisk',
                title: 'On disk',
                tooltip: "Shows disk usage for active resource",
                nav: isGlobalAdmin(),
                dynamicHash: appUrls.statusStorageOnDisk
            }),
            new leafMenuItem({
                route: 'databases/status/storage/storageBreakdown',
                moduleId: 'viewmodels/database/status/storage/statusStorageBreakdown',
                title: 'Internal storage Breakdown',
                tooltip: "Shows detailed information about internal storage breakdown",
                nav: isGlobalAdmin(),
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
        ], 'icon-plus'),
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
                route: 'databases/status/debug/dataSubscriptions',
                moduleId: 'viewmodels/database/status/debug/statusDebugDataSubscriptions',
                title: 'Data subscriptions',
                tooltip: "Shows information about data subscriptions",
                nav: true,
                dynamicHash: appUrls.dataSubscriptions
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
                nav: isGlobalAdmin(),
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
                route: 'databases/status/debug/persist',
                moduleId: 'viewmodels/database/status/debug/statusDebugPersistAutoIndex',
                title: 'Persist auto index',
                tooltip: "Persists auto index",
                nav: true,
                dynamicHash: appUrls.statusDebugPersistAutoIndex
            }),
            new leafMenuItem({
                route: 'databases/status/debug/explainReplication',
                moduleId: 'viewmodels/database/status/debug/statusDebugExplainReplication',
                title: 'Explain replication',
                tooltip: "Shows information about replication of given document to given replication destination",
                nav: activeDatabase() && activeDatabase().isBundleActive("Replication"),
                dynamicHash: appUrls.statusDebugExplainReplication
            })
        ], 'icon-plus'),
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
            nav: canExposeConfigOverTheWire(),
            dynamicHash: appUrls.requestsTracing
        }),
        new leafMenuItem({
            route: 'databases/status',
            moduleId: 'viewmodels/database/status/statistics',
            title: 'Stats',
            nav: true,
            dynamicHash: appUrls.status
        }),
        new leafMenuItem({
            route: 'databases/status/logs',
            moduleId: 'viewmodels/database/status/logs',
            title: 'Logs',
            nav: true,
            dynamicHash: appUrls.logs
        }),
        new leafMenuItem({
            route: 'databases/status/runningTasks',
            moduleId: 'viewmodels/database/status/runningTasks',
            title: 'Running Tasks',
            nav: true,
            dynamicHash: appUrls.runningTasks
        }),
        new leafMenuItem({
            route: 'databases/status/alerts',
            moduleId: 'viewmodels/database/status/alerts',
            title: 'Alerts',
            nav: true,
            dynamicHash: appUrls.alerts
        }),
        new leafMenuItem({
            route: 'databases/status/indexErrors',
            moduleId: 'viewmodels/database/status/indexErrors',
            title: 'Index Errors',
            nav: true,
            dynamicHash: appUrls.indexErrors
        }),
        new leafMenuItem({
            route: 'databases/status/replicationStats',
            moduleId: 'viewmodels/database/status/replicationStats',
            title: 'Replication Stats',
            nav: true,
            dynamicHash: appUrls.replicationStats
        }),
        new leafMenuItem({
            route: 'databases/status/sqlReplicationPerfStats',
            moduleId: 'viewmodels/database/status/sqlReplicationPerfStats',
            title: 'SQL Replication Stats',
            nav: true,
            dynamicHash: appUrls.sqlReplicationPerfStats
        }),
        new leafMenuItem({
            route: 'databases/status/userInfo',
            moduleId: 'viewmodels/database/status/userInfo',
            title: 'User Info',
            nav: true,
            dynamicHash: appUrls.userInfo
        }),
        new leafMenuItem({
            route: 'databases/status/visualizer',
            moduleId: 'viewmodels/database/status/visualizer',
            title: 'Map/Reduce Visualizer',
            nav: true,
            dynamicHash: appUrls.visualizer
        }),
        new leafMenuItem({
            route: 'databases/status/debug*details',
            moduleId: 'viewmodels/database/status/debug/statusDebug',
            title: 'Debug',
            nav: true,
            dynamicHash: appUrls.statusDebug
        }),
        new leafMenuItem({
            route: 'databases/status/storage*details',
            moduleId: 'viewmodels/database/status/storage/statusStorage',
            title: 'Storage',
            nav: true,
            dynamicHash: appUrls.statusStorageOnDisk
        }),
        new leafMenuItem({
            route: 'databases/status/infoPackage',
            moduleId: 'viewmodels/manage/infoPackage',
            title: 'Gather Debug Info',
            nav: canExposeConfigOverTheWire(),
            dynamicHash: appUrls.infoPackage
        })
    ];

    return new intermediateMenuItem("Stats", items, "icon-stats");
}

function getSettingsMenuItem(appUrls: computedAppUrls) {
    var items: menuItem[] = [
        new leafMenuItem({
            route: ['databases/settings', 'databases/settings/databaseSettings'],
            moduleId: 'viewmodels/database/settings/databaseSettings',
            title: 'Database Settings',
            nav: true,
            css: 'icon-plus',
            dynamicHash: appUrls.databaseSettings
        }),
        new leafMenuItem({
            route: 'databases/settings/quotas',
            moduleId: 'viewmodels/database/settings/quotas',
            title: 'Quotas',
            nav: true,
            css: 'icon-plus',
            dynamicHash: appUrls.quotas
        }),
        new leafMenuItem({
            route: 'databases/settings/replication',
            moduleId: 'viewmodels/database/settings/replications',
            title: 'Replication',
            nav: true,
            css: 'icon-plus',
            dynamicHash: appUrls.replications
        }),
        new leafMenuItem({
            route: 'databases/settings/etl',
            moduleId: 'viewmodels/database/settings/etl',
            title: 'ETL',
            nav: true,
            css: 'icon-plus',
            dynamicHash: appUrls.etl
        }),
        new leafMenuItem({
            route: 'databases/settings/sqlReplication',
            moduleId: 'viewmodels/database/settings/sqlReplications',
            title: 'SQL Replication',
            nav: true,
            css: 'icon-plus',
            dynamicHash: appUrls.sqlReplications
        }),
        new leafMenuItem({
            route: 'databases/settings/editSqlReplication(/:sqlReplicationName)',
            moduleId: 'viewmodels/database/settings/editSqlReplication',
            title: 'Edit SQL Replication',
            nav: false,
            css: 'icon-plus',
            dynamicHash: appUrls.editSqlReplication
        }),
        new leafMenuItem({
            route: 'databases/settings/sqlReplicationConnectionStringsManagement',
            moduleId: 'viewmodels/database/settings/sqlReplicationConnectionStringsManagement',
            title: 'SQL Replication Connection Strings',
            nav: false,
            css: 'icon-plus',
            dynamicHash: appUrls.sqlReplicationsConnections
        }),
        new leafMenuItem({
            route: 'databases/settings/versioning',
            moduleId: 'viewmodels/database/settings/versioning',
            title: 'Versioning',
            nav: true,
            css: 'icon-plus',
            dynamicHash: appUrls.versioning
        }),
        new leafMenuItem({
            route: 'databases/settings/periodicExport',
            moduleId: 'viewmodels/database/settings/periodicExport',
            title: 'Periodic Export',
            nav: true,
            css: 'icon-plus',
            dynamicHash: appUrls.periodicExport
        }),
        new leafMenuItem({
            route: 'databases/settings/customFunctionsEditor',
            moduleId: 'viewmodels/database/settings/customFunctionsEditor',
            title: 'Custom Functions',
            nav: true,
            css: 'icon-plus',
            dynamicHash: appUrls.customFunctionsEditor
        }),
        new leafMenuItem({
            route: 'databases/settings/databaseStudioConfig',
            moduleId: 'viewmodels/databaseStudioConfig',
            title: 'Studio Config',
            nav: true,
            css: 'icon-plus',
            dynamicHash: appUrls.databaseStudioConfig
        })
    ];

    return new intermediateMenuItem('Settings', items, 'icon-settings');
}

function getTasksMenuItem(appUrls: computedAppUrls, activeDatabase: KnockoutObservable<database>) {
    var importDatabaseUrl = ko.computed(() => appUrl.forImportDatabase(activeDatabase()));
    var exportDatabaseUrl = ko.computed(() => appUrl.forExportDatabase(activeDatabase()));
    var toggleIndexingUrl = ko.computed(() => appUrl.forToggleIndexing(activeDatabase()));
    var setAcknowledgedEtagUrl = ko.computed(() => appUrl.forSetAcknowledgedEtag(activeDatabase()));
    var sampleDataUrl = ko.computed(() => appUrl.forSampleData(activeDatabase()));
    var csvImportUrl = ko.computed(() => appUrl.forCsvImport(activeDatabase()));

    var submenu: leafMenuItem[] = [
        new leafMenuItem({
            route: [
                'databases/tasks',
                'databases/tasks/importDatabase'
            ],
            moduleId: 'viewmodels/database/tasks/importDatabase',
            title: 'Import Database',
            nav: true,
            css: 'icon-plus',
            dynamicHash: importDatabaseUrl
        }),
        new leafMenuItem({
            route: 'databases/tasks/exportDatabase',
            moduleId: 'viewmodels/database/tasks/exportDatabase',
            title: 'Export Database',
            nav: true,
            css: 'icon-plus',
            dynamicHash: exportDatabaseUrl
        }),
        new leafMenuItem({
            route: 'databases/tasks/toggleIndexing',
            moduleId: 'viewmodels/database/tasks/toggleIndexing',
            title: 'Toggle Indexing',
            nav: true,
            css: 'icon-plus',
            dynamicHash: toggleIndexingUrl
        }),
        new leafMenuItem({
            route: 'databases/tasks/subscriptionsTask',
            moduleId: 'viewmodels/database/tasks/subscriptionsTask',
            title: 'Subscriptions',
            nav: activeDatabase() && activeDatabase().isAdminCurrentTenant(),
            css: 'icon-plus',
            dynamicHash: setAcknowledgedEtagUrl
        }),
        new leafMenuItem({
            route: 'databases/tasks/sampleData',
            moduleId: 'viewmodels/database/tasks/createSampleData',
            title: 'Create Sample Data',
            nav: true,
            css: 'icon-plus',
            dynamicHash: sampleDataUrl
        }),
        new leafMenuItem({
            route: 'databases/tasks/csvImport',
            moduleId: 'viewmodels/database/tasks/csvImport',
            title: 'CSV Import',
            nav: true,
            css: 'icon-plus',
            dynamicHash: csvImportUrl
        })
    ];

    return new intermediateMenuItem('Tasks', submenu, 'icon-tasks');
}

function getQueryMenuItem(appUrls: computedAppUrls) {
    var routes: leafMenuItem[] = [
        new leafMenuItem({
            route: ['', 'databases/query/index(/:indexNameOrRecentQueryIndex)'],
            moduleId: 'viewmodels/database/query/query',
            title: 'Query',
            nav: true,
            css: 'icon-plus',
            dynamicHash: appUrls.query('')
        }),
        new leafMenuItem({
            route: 'databases/query/reporting(/:indexName)',
            moduleId: 'viewmodels/database/reporting/reporting',
            title: 'Reporting',
            nav: true,
            css: 'icon-plus',
            dynamicHash: appUrls.reporting
        }),
        new leafMenuItem({
            route: 'databases/query/exploration',
            moduleId: 'viewmodels/database/exploration/exploration',
            title: "Data exploration",
            nav: true,
            css: 'icon-plus',
            dynamicHash: appUrls.exploration
        })
    ];

    return new intermediateMenuItem("Query", routes, 'icon-query');
}

function getIndexesMenuItem(appUrls: computedAppUrls) {
    let indexesChildren = [
        new leafMenuItem({
            title: "Indexes",
            nav: true,
            route: "databases/indexes",
            moduleId: "viewmodels/database/indexes/indexes",
            css: 'icon-plus',
            dynamicHash: appUrls.indexes
        }),
        new leafMenuItem({
            title: "Index merge suggestions",
            nav: true,
            route: "databases/indexes/mergeSuggestions",
            moduleId: "viewmodels/database/indexes/indexMergeSuggestions",
            css: 'icon-plus',
            dynamicHash: appUrls.megeSuggestions
        }),
        new leafMenuItem({
            title: 'Edit Index',
            route: 'databases/indexes/edit(/:indexName)',
            moduleId: 'viewmodels/database/indexes/editIndex',
            css: 'icon-plus',
            nav: false
        }),
        new leafMenuItem({
            title: 'Terms',
            route: 'databases/indexes/terms/(:indexName)',
            moduleId: 'viewmodels/database/indexes/indexTerms',
            css: 'icon-plus',
            nav: false
        }),
        new leafMenuItem({
            title: 'Transformers',
            route: 'databases/transformers',
            moduleId: 'viewmodels/database/transformers/transformers',
            css: 'icon-plus',
            nav: true,
            dynamicHash: appUrls.transformers
        }),
        new leafMenuItem({
            route: 'databases/transformers/edit(/:transformerName)',
            moduleId: 'viewmodels/database/transformers/editTransformer',
            title: 'Edit Transformer',
            css: 'icon-plus',
            nav: false
        })
    ];

    return new intermediateMenuItem("Indexes", indexesChildren, 'icon-indexes');
}

function getDocumentsMenuItem(appUrls: computedAppUrls) {
    let documentsChildren = [
        new leafMenuItem({
            title: "Documents",
            nav: true,
            route: "databases/documents",
            moduleId: "viewmodels/database/documents/documents",
            hash: appUrls.documents,
            css: 'icon-plus'
        }),
        new leafMenuItem({
            title: "Conflicts",
            nav: true,
            route: "database/conflicts",
            moduleId: "viewmodels/database/conflicts/conflicts",
            css: 'icon-plus',
            hash: appUrls.conflicts
        }),
        new leafMenuItem({
            title: "Patch",
            nav: true,
            route: "databases/patch(/:recentPatchHash)",
            moduleId: "viewmodels/database/patch/patch",
            css: 'icon-plus',
            hash: appUrls.patch
        }),
        new leafMenuItem({
            route: "databases/edit",
            title: "Edit Document",
            moduleId: "viewmodels/database/documents/editDocument",
            nav: false
        })
    ];

    return new intermediateMenuItem("Documents", documentsChildren, "icon-documents");
}

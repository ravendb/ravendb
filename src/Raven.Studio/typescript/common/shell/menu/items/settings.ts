import intermediateMenuItem = require("common/shell/menu/intermediateMenuItem");
import leafMenuItem = require("common/shell/menu/leafMenuItem");
import separatorMenuItem = require("common/shell/menu/separatorMenuItem");
import accessHelper = require("viewmodels/shell/accessHelper");
export = getSettingsMenuItem;

function getSettingsMenuItem(appUrls: computedAppUrls) {
    const items: menuItem[] = [
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
        /* TODO - bring this back for RTM - issue 8429
        new leafMenuItem({
            route: 'databases/settings/connectionStrings',
            moduleId: "viewmodels/database/settings/connectionStrings",
            title: "Connection Strings",
            nav: true,
            css: 'icon-manage-connection-strings',
            dynamicHash: appUrls.connectionStrings,
            enabled: accessHelper.isGlobalAdmin
        }),*/
        new leafMenuItem({
            route: 'databases/settings/revisions',
            moduleId: 'viewmodels/database/settings/revisions',
            title: 'Document Revisions',
            nav: true,
            css: 'icon-revisions',
            dynamicHash: appUrls.revisions
        }),
        new leafMenuItem({
            route: 'databases/settings/clientConfiguration',
            moduleId: 'viewmodels/database/settings/clientConfiguration',
            title: 'Client Configuration',
            nav: true,
            css: 'icon-client-configuration',
            dynamicHash: appUrls.clientConfiguration
        }),
        new leafMenuItem({
            route: 'databases/manageDatabaseGroup',
            moduleId: 'viewmodels/resources/manageDatabaseGroup',
            title: 'Manage database group',
            nav: true,
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
        new separatorMenuItem(),
        new separatorMenuItem('Tasks'),
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
            route: 'databases/tasks/editRavenEtlTask',
            moduleId: 'viewmodels/database/tasks/editRavenEtlTask',
            title: 'RavenDB ETL Task',
            nav: false,
            dynamicHash: appUrls.editRavenEtlTaskUrl,
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
        new leafMenuItem({
            route: 'databases/tasks/ongoingTasks',
            moduleId: 'viewmodels/database/tasks/ongoingTasks',
            title: 'Manage Ongoing Tasks',
            nav: true,
            css: 'icon-manage-ongoing-tasks',
            dynamicHash: appUrls.ongoingTasksUrl
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
    ];

    return new intermediateMenuItem('Settings', items, 'icon-settings');
}

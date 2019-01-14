import intermediateMenuItem = require("common/shell/menu/intermediateMenuItem");
import leafMenuItem = require("common/shell/menu/leafMenuItem");
import separatorMenuItem = require("common/shell/menu/separatorMenuItem");
import accessManager = require("common/shell/accessManager");

export = getSettingsMenuItem;

function getSettingsMenuItem(appUrls: computedAppUrls) {

    const access = accessManager.default.databaseSettingsMenu;
    
    const items: menuItem[] = [
        new leafMenuItem({
            route: ['databases/record', 'databases/settings/databaseRecord'],
            moduleId: 'viewmodels/database/settings/databaseRecord',
            title: 'Database Record',
            nav: access.showDatabaseRecordMenuItem,
            css: 'icon-database-settings',
            dynamicHash: appUrls.databaseRecord
        }),
        new leafMenuItem({
            route: 'databases/settings/connectionStrings',
            moduleId: "viewmodels/database/settings/connectionStrings",
            title: "Connection Strings",
            nav: access.showConnectionStringsMenuItem,
            css: 'icon-manage-connection-strings',
            dynamicHash: appUrls.connectionStrings,
            enabled: access.enableConnectionStringsMenuItem
        }),
        new leafMenuItem({
            route: 'databases/settings/conflictResolution',
            moduleId: "viewmodels/database/settings/conflictResolution",
            title: "Conflict Resolution",
            nav: true,
            css: 'icon-conflicts-resolution',
            dynamicHash: appUrls.conflictResolution,
            enabled: access.enableConflictResolutionMenuItem
        }),
        new leafMenuItem({
            route: 'databases/settings/clientConfiguration',
            moduleId: 'viewmodels/database/settings/clientConfiguration',
            title: 'Client Configuration',
            nav: true,
            css: 'icon-database-client-configuration',
            dynamicHash: appUrls.clientConfiguration
        }),
        new leafMenuItem({
            route: 'databases/settings/studioConfiguration',
            moduleId: 'viewmodels/database/settings/studioConfiguration',
            title: 'Studio Configuration',
            nav: true,
            css: 'icon-database-studio-configuration',
            dynamicHash: appUrls.studioConfiguration
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
            route: 'databases/settings/expiration',
            moduleId: 'viewmodels/database/settings/expiration',
            title: 'Document Expiration',
            nav: true,
            css: 'icon-document-expiration',
            dynamicHash: appUrls.expiration
        }),
        new leafMenuItem({
            route: 'databases/settings/customSorters',
            moduleId: 'viewmodels/database/settings/customSorters',
            title: 'Custom Sorters',
            nav: true,
            css: 'icon-custom-sorters',
            dynamicHash: appUrls.customSorters
        }),
        new leafMenuItem({
            route: 'databases/settings/editCustomSorter',
            moduleId: 'viewmodels/database/settings/editCustomSorter',
            title: 'Custom Sorter',
            nav: false,
            dynamicHash: appUrls.editCustomSorter, 
            itemRouteToHighlight: 'databases/settings/customSorters'
        }),
        new leafMenuItem({
            route: 'databases/manageDatabaseGroup',
            moduleId: 'viewmodels/resources/manageDatabaseGroup',
            title: 'Manage Database Group',
            nav: true,
            css: 'icon-manage-dbgroup',
            dynamicHash: appUrls.manageDatabaseGroup
        }),
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
            route: 'databases/tasks/editSqlEtlTask',
            moduleId: 'viewmodels/database/tasks/editSqlEtlTask',
            title: 'SQL ETL Task',
            nav: false,
            dynamicHash: appUrls.editSqlEtlTaskUrl,
            itemRouteToHighlight: 'databases/tasks/ongoingTasks'
        }),
        new leafMenuItem({
            route: 'databases/tasks/import*details',
            moduleId: 'viewmodels/database/tasks/importParent',
            title: 'Import Data',
            nav: true,
            css: 'icon-import-database',
            dynamicHash: appUrls.importDatabaseFromFileUrl
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
        })
    ];

    return new intermediateMenuItem('Settings', items, 'icon-settings');
}

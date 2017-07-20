import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");
import appUrl = require("common/appUrl");
import intermediateMenuItem = require("common/shell/menu/intermediateMenuItem");
import leafMenuItem = require("common/shell/menu/leafMenuItem");

export = getTasksMenuItem;

function getTasksMenuItem(appUrls: computedAppUrls) {
    let activeDatabase = activeDatabaseTracker.default.database;
    const importDatabaseUrl = ko.pureComputed(() => appUrl.forImportDatabase(activeDatabase()));
    const exportDatabaseUrl = ko.pureComputed(() => appUrl.forExportDatabase(activeDatabase()));
    const sampleDataUrl = ko.pureComputed(() => appUrl.forSampleData(activeDatabase()));
    const ongoingTasksUrl = ko.pureComputed(() => appUrl.forOngoingTasks(activeDatabase()));
    const editExternalReplicationTaskUrl = ko.pureComputed(() => appUrl.forEditExternalReplication(activeDatabase()));
    const editSubscriptionTaskUrl = ko.pureComputed(() => appUrl.forEditSubscription(activeDatabase()));
    const csvImportUrl = ko.pureComputed(() => appUrl.forCsvImport(activeDatabase()));

    const submenu: leafMenuItem[] = [
        new leafMenuItem({
            route: [
                'databases/tasks',
                'databases/tasks/importDatabase'
            ],
            moduleId: 'viewmodels/database/tasks/importDatabase',
            title: 'Import Database',
            nav: true,
            css: 'icon-import-database',
            dynamicHash: importDatabaseUrl
        }),
        new leafMenuItem({
            route: 'databases/tasks/exportDatabase',
            moduleId: 'viewmodels/database/tasks/exportDatabase',
            title: 'Export Database',
            nav: true,
            css: 'icon-export-database',
            dynamicHash: exportDatabaseUrl
        }),
        new leafMenuItem({
            route: 'databases/tasks/sampleData',
            moduleId: 'viewmodels/database/tasks/createSampleData',
            title: 'Create Sample Data',
            nav: true,
            css: 'icon-create-sample-data',
            dynamicHash: sampleDataUrl
        }),
        new leafMenuItem({
            route: 'databases/tasks/ongoingTasks',
            moduleId: 'viewmodels/database/tasks/ongoingTasks',
            title: 'Manage Ongoing Tasks',
            nav: true,
            css: 'icon-manage-ongoing-tasks', 
            dynamicHash: ongoingTasksUrl
        }),
        new leafMenuItem({
            route: 'databases/tasks/editExternalReplicationTask', 
            moduleId: 'viewmodels/database/tasks/editExternalReplicationTask',
            title: 'External Replication Task',
            nav: false,
            dynamicHash: editExternalReplicationTaskUrl,
            itemRouteToHighlight: 'databases/tasks/ongoingTasks'
        }),
        new leafMenuItem({
            route: 'databases/tasks/editPeriodicBackupTask', 
            moduleId: 'viewmodels/database/tasks/editPeriodicBackupTask',
            title: 'Backup Task',
            nav: false,
            dynamicHash: editExternalReplicationTaskUrl,
            itemRouteToHighlight: 'databases/tasks/ongoingTasks'
        }),
        new leafMenuItem({
            route: 'databases/tasks/editSubscriptionTask',
            moduleId: 'viewmodels/database/tasks/editSubscriptionTask',
            title: 'Subscription Task',
            nav: false,
            dynamicHash: editSubscriptionTaskUrl,
            itemRouteToHighlight: 'databases/tasks/ongoingTasks'
        })
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

    return new intermediateMenuItem('Tasks', submenu, 'icon-tasks');
}


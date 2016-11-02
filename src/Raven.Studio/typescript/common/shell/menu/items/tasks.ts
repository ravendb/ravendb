import activeResourceTracker = require("common/shell/activeResourceTracker");
import appUrl = require("common/appUrl");
import intermediateMenuItem = require("common/shell/menu/intermediateMenuItem");
import leafMenuItem = require("common/shell/menu/leafMenuItem");

export = getTasksMenuItem;

function getTasksMenuItem(appUrls: computedAppUrls) {
    let activeDatabase = activeResourceTracker.default.database;
    var importDatabaseUrl = ko.computed(() => appUrl.forImportDatabase(activeDatabase()));
    var exportDatabaseUrl = ko.computed(() => appUrl.forExportDatabase(activeDatabase()));
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
        /* TODO:
        new leafMenuItem({
            route: 'databases/tasks/subscriptionsTask',
            moduleId: 'viewmodels/database/tasks/subscriptionsTask',
            title: 'Subscriptions',
            nav: activeDatabase() && activeDatabase().isAdminCurrentTenant(),
            css: 'icon-plus',
            dynamicHash: setAcknowledgedEtagUrl
        }),*/
        new leafMenuItem({
            route: 'databases/tasks/sampleData',
            moduleId: 'viewmodels/database/tasks/createSampleData',
            title: 'Create Sample Data',
            nav: true,
            css: 'icon-plus',
            dynamicHash: sampleDataUrl
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

    return new intermediateMenuItem('Tasks', submenu, 'icon-tasks');
}


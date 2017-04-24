import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");
import appUrl = require("common/appUrl");
import intermediateMenuItem = require("common/shell/menu/intermediateMenuItem");
import leafMenuItem = require("common/shell/menu/leafMenuItem");

export = getTasksMenuItem;

function getTasksMenuItem(appUrls: computedAppUrls) {
    let activeDatabase = activeDatabaseTracker.default.database;
    var importDatabaseUrl = ko.pureComputed(() => appUrl.forImportDatabase(activeDatabase()));
    var exportDatabaseUrl = ko.pureComputed(() => appUrl.forExportDatabase(activeDatabase()));
    var sampleDataUrl = ko.pureComputed(() => appUrl.forSampleData(activeDatabase()));
    var csvImportUrl = ko.pureComputed(() => appUrl.forCsvImport(activeDatabase()));

    var submenu: leafMenuItem[] = [
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


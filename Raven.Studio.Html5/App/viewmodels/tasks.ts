import durandalRouter = require("plugins/router");
import database = require("models/database");
import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");

class tasks extends viewModelBase {

    router: DurandalRootRouter = null;
    activeSubViewTitle: KnockoutComputed<string>;
    
    constructor() {
        super();

        var importDatabaseUrl = ko.computed(() => appUrl.forImportDatabase(this.activeDatabase()));
        var exportDatabaseUrl = ko.computed(() => appUrl.forExportDatabase(this.activeDatabase()));
        var backupDatabaseUrl = ko.computed(() => appUrl.forBackupDatabase(this.activeDatabase()));
        var restoreDatabaseUrl = ko.computed(() => appUrl.forRestoreDatabase(this.activeDatabase()));
        var toggleIndexingUrl = ko.computed(() => appUrl.forToggleIndexing(this.activeDatabase()));
        var sampleDataUrl = ko.computed(() => appUrl.forSampleData(this.activeDatabase()));
        var csvImportUrl = ko.computed(() => appUrl.forCsvImport(this.activeDatabase()));

        this.router = durandalRouter.createChildRouter()
            .map([
                { route: ['tasks', 'tasks/importDatabase'], moduleId: 'viewmodels/importDatabase', title: 'Import Database', nav: true, hash: importDatabaseUrl },
                { route: 'tasks/exportDatabase', moduleId: 'viewmodels/exportDatabase', title: 'Export Database', nav: true, hash: exportDatabaseUrl },
                { route: 'tasks/backupDatabase', moduleId: 'viewmodels/backupDatabase', title: 'Backup Database', nav: true, hash: backupDatabaseUrl },
                { route: 'tasks/restoreDatabase', moduleId: 'viewmodels/restoreDatabase', title: 'Restore Database', nav: true, hash: restoreDatabaseUrl },
                { route: 'tasks/toggleIndexing', moduleId: 'viewmodels/toggleIndexing', title: 'Toggle Indexing', nav: true, hash: toggleIndexingUrl },
                { route: 'tasks/sampleData', moduleId: 'viewmodels/createSampledata', title: 'Create Sample Data', nav: true, hash: sampleDataUrl },
                { route: 'tasks/csvImport', moduleId: 'viewmodels/csvImport', title: 'CSV Import', nav: true, hash: csvImportUrl }
            ])
            .buildNavigationModel();

        this.activeSubViewTitle = ko.computed(() => {
            // Is there a better way to get the active route?
            var activeRoute = this.router.navigationModel().first(r => r.isActive());
            return activeRoute != null ? activeRoute.title : "";
        });
    }

    activate(args: any) {
        super.activate(args);
    }
}

export = tasks;
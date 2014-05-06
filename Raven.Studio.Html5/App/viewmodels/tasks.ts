import durandalRouter = require("plugins/router");
import database = require("models/database");
import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");

class tasks extends viewModelBase {

    router: DurandalRootRouter = null;
    isOnSystemDatabase: KnockoutComputed<boolean>;
    isOnUserDatabase: KnockoutComputed<boolean>;
    activeSubViewTitle: KnockoutComputed<string>;
    appUrls: computedAppUrls;

    constructor() {
        super();

        this.isOnSystemDatabase = ko.computed(() => this.activeDatabase() && this.activeDatabase().isSystem);
        this.isOnUserDatabase = ko.computed(() => this.activeDatabase() && !this.isOnSystemDatabase());
        this.appUrls = appUrl.forCurrentDatabase();

        var importDatabaseUrl = ko.computed(()=> appUrl.forImportDatabase(this.activeDatabase()));
        var exportDatabaseUrl = ko.computed(()=> appUrl.forExportDatabase(this.activeDatabase()));
        var backupDatabaseUrl = ko.computed(()=> appUrl.forBackupDatabase(this.activeDatabase()));
        var restoreDatabaseUrl = ko.computed(()=> appUrl.forRestoreDatabase(this.activeDatabase()));
        var toggleIndexingUrl = ko.computed(()=> appUrl.forToggleIndexing(this.activeDatabase()));
        var sampleDataUrl = ko.computed(()=> appUrl.forSampleData(this.activeDatabase()));
        var csvImportUrl = ko.computed(()=> appUrl.forCsvImport(this.activeDatabase()));

        this.router = durandalRouter.createChildRouter()
            .map([
            { route: ['databases/tasks', 'databases/tasks/importDatabase'], moduleId: 'viewmodels/importDatabase', title: 'Import Database', nav: true, hash: importDatabaseUrl },
            { route: 'databases/tasks/exportDatabase', moduleId: 'viewmodels/exportDatabase', title: 'Export Database', nav: true, hash: exportDatabaseUrl },
            { route: 'databases/tasks/backupDatabase', moduleId: 'viewmodels/backupDatabase', title: 'Backup Database', nav: true, hash: backupDatabaseUrl },
            { route: 'databases/tasks/restoreDatabase', moduleId: 'viewmodels/restoreDatabase', title: 'Restore Database', nav: true, hash: restoreDatabaseUrl },
            { route: 'databases/tasks/toggleIndexing', moduleId: 'viewmodels/toggleIndexing', title: 'Toggle Indexing', nav: true, hash: toggleIndexingUrl },
            { route: 'databases/tasks/sampleData', moduleId: 'viewmodels/createSampledata', title: 'Create Sample Data', nav: true, hash: sampleDataUrl },
            { route: 'databases/tasks/csvImport', moduleId: 'viewmodels/csvImport', title: 'CSV Import', nav: true, hash: csvImportUrl }
            ])
            .buildNavigationModel();

        this.activeSubViewTitle = ko.computed(()=> {
            // Is there a better way to get the active route?
            var activeRoute = this.router.navigationModel().first(r=> r.isActive());
            return activeRoute != null ? activeRoute.title : "";
        });
    }

    activate(args: any) {
        super.activate(args);
    }

    routeIsVisible(route: DurandalRouteConfiguration) {
        var systemOnlyPages = ["Restore Database", "Database Settings", "Replication", "SQL Replication"];
        if (jQuery.inArray(route.title, systemOnlyPages) !== -1) {
            return this.isOnSystemDatabase();
        } else {
            return true;
        }
    }
}

export = tasks;
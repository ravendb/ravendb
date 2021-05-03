import viewModelBase = require("viewmodels/viewModelBase");
import appUrl = require("common/appUrl");

import durandalRouter = require("plugins/router");

class importParent {
    router: DurandalRootRouter;

    constructor() {
        this.router = durandalRouter.createChildRouter()
            .map([
                {
                    route: 'databases/tasks/import/file',
                    moduleId: 'viewmodels/database/tasks/importDatabaseFromFile',
                    title: 'Import database from file',
                    nav: true,
                    tabName: "From file (.ravendbdump)",
                    dynamicHash: appUrl.forCurrentDatabase().importDatabaseFromFileUrl,
                    requiredAccess: "DatabaseReadWrite"
                },
                {
                    route: 'databases/tasks/import/migrateRavenDB',
                    moduleId: 'viewmodels/database/tasks/migrateRavenDbDatabase',
                    title: 'Import database from another RavenDB',
                    tabName: "From RavenDB",
                    nav: true,
                    dynamicHash: appUrl.forCurrentDatabase().migrateRavenDbDatabaseUrl,
                    requiredAccess: "DatabaseReadWrite"
                },
                {
                    route: 'databases/tasks/import/csv',
                    moduleId: 'viewmodels/database/tasks/importCollectionFromCsv',
                    title: 'Import collection from CSV file',
                    tabName: "From CSV file",
                    nav: true,
                    dynamicHash: appUrl.forCurrentDatabase().importCollectionFromCsv,
                    requiredAccess: "DatabaseReadWrite"
                },
                {
                    route: 'databases/tasks/import/sql',
                    moduleId: 'viewmodels/database/tasks/importDatabaseFromSql',
                    title: 'Import from SQL Database',
                    tabName: "From SQL",
                    nav: true,
                    dynamicHash: appUrl.forCurrentDatabase().importDatabaseFromSql,
                    requiredAccess: "DatabaseReadWrite"
                },
                {
                    route: 'databases/tasks/import/migrate',
                    moduleId: 'viewmodels/database/tasks/migrateDatabase',
                    title: 'Migrate database',
                    tabName: "From other",
                    nav: true,
                    dynamicHash: appUrl.forCurrentDatabase().migrateDatabaseUrl,
                    requiredAccess: "DatabaseReadWrite"
                }
            ])
            .buildNavigationModel();
    }
}

export = importParent; 

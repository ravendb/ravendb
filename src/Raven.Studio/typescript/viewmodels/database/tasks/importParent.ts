import viewModelBase = require("viewmodels/viewModelBase");
import appUrl = require("common/appUrl");

import durandalRouter = require("plugins/router");

class importParent extends viewModelBase {
    router: DurandalRootRouter;

    constructor() {
        super();
        
        this.router = durandalRouter.createChildRouter()
            .map([
                {
                    route: 'databases/tasks/import/file',
                    moduleId: 'viewmodels/database/tasks/importDatabaseFromFile',
                    title: 'Import database from file',
                    nav: true,
                    tabName: "From file (.ravendbdump)",
                    dynamicHash: appUrl.forCurrentDatabase().importDatabaseFromFileUrl
                },
                {
                    route: 'databases/tasks/import/migrateRavenDB',
                    moduleId: 'viewmodels/database/tasks/migrateRavenDbDatabase',
                    title: 'Import database from another RavenDB',
                    tabName: "From another RavenDB Server",
                    nav: true,
                    dynamicHash: appUrl.forCurrentDatabase().migrateRavenDbDatabaseUrl
                },
                {
                    route: 'databases/tasks/import/csv',
                    moduleId: 'viewmodels/database/tasks/importCollectionFromCsv',
                    title: 'Import collection from CSV file',
                    tabName: "From CSV file",
                    nav: true,
                    dynamicHash: appUrl.forCurrentDatabase().importCollectionFromCsv
                },
                {
                    route: 'databases/tasks/import/sql',
                    moduleId: 'viewmodels/database/tasks/importDatabaseFromSql',
                    title: 'Import from SQL Database',
                    tabName: "From SQL",
                    nav: true,
                    dynamicHash: appUrl.forCurrentDatabase().importDatabaseFromSql
                },
                {
                    route: 'databases/tasks/import/migrate',
                    moduleId: 'viewmodels/database/tasks/migrateDatabase',
                    title: 'Migrate database',
                    tabName: "From another database",
                    nav: true,
                    dynamicHash: appUrl.forCurrentDatabase().migrateDatabaseUrl
                }
            ])
            .buildNavigationModel();
    }
}

export = importParent; 

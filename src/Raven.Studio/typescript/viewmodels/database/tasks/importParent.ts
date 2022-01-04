import appUrl = require("common/appUrl");

import durandalRouter = require("plugins/router");

class importParent {
    
    view = require("views/database/tasks/importParent.html");
    
    getView() {
        return this.view;
    }
    
    router: DurandalRootRouter;

    constructor() {
        this.router = durandalRouter.createChildRouter()
            .map([
                {
                    route: 'databases/tasks/import/file',
                    moduleId: require('viewmodels/database/tasks/importDatabaseFromFile'),
                    title: 'Import database from file',
                    nav: true,
                    tabName: "From file (.ravendbdump)",
                    dynamicHash: appUrl.forCurrentDatabase().importDatabaseFromFileUrl,
                    requiredAccess: "DatabaseReadWrite"
                },
                {
                    route: 'databases/tasks/import/migrateRavenDB',
                    moduleId: require('viewmodels/database/tasks/migrateRavenDbDatabase'),
                    title: 'Import database from another RavenDB',
                    tabName: "From RavenDB Server",
                    nav: true,
                    dynamicHash: appUrl.forCurrentDatabase().migrateRavenDbDatabaseUrl,
                    requiredAccess: "DatabaseReadWrite"
                },
                {
                    route: 'databases/tasks/import/csv',
                    moduleId: require('viewmodels/database/tasks/importCollectionFromCsv'),
                    title: 'Import collection from CSV file',
                    tabName: "From CSV File",
                    nav: true,
                    dynamicHash: appUrl.forCurrentDatabase().importCollectionFromCsv,
                    requiredAccess: "DatabaseReadWrite"
                },
                {
                    route: 'databases/tasks/import/sql',
                    moduleId: require('viewmodels/database/tasks/importDatabaseFromSql'),
                    title: 'Import from SQL Database',
                    tabName: "From SQL",
                    nav: true,
                    dynamicHash: appUrl.forCurrentDatabase().importDatabaseFromSql,
                    requiredAccess: "DatabaseReadWrite"
                },
                {
                    route: 'databases/tasks/import/migrate',
                    moduleId: require('viewmodels/database/tasks/migrateDatabase'),
                    title: 'Migrate database',
                    tabName: "From NoSQL",
                    nav: true,
                    dynamicHash: appUrl.forCurrentDatabase().migrateDatabaseUrl,
                    requiredAccess: "DatabaseReadWrite"
                }
            ])
            .buildNavigationModel();
    }
}

export = importParent; 

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
                    hash: appUrl.forCurrentDatabase().importDatabaseFromFileUrl
                },
                {
                    route: 'databases/tasks/import/migrate',
                    moduleId: 'viewmodels/database/tasks/migrateDatabase',
                    title: 'Import database from another RavenDB',
                    tabName: "From another RavenDB Server",
                    nav: true,
                    hash: appUrl.forCurrentDatabase().migrateDatabaseUrl
                },
                {
                    route: 'databases/tasks/import/csv',
                    moduleId: 'viewmodels/database/tasks/importCollectionFromCsv',
                    title: 'Import collection from CSV file',
                    tabName: "From CSV file",
                    nav: true,
                    hash: appUrl.forCurrentDatabase().importCollectionFromCsv
                }
            ])
            .buildNavigationModel();
    }
}

export = importParent; 

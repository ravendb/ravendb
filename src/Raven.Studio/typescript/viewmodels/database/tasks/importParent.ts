import appUrl = require("common/appUrl");

import durandalRouter = require("plugins/router");
import shardingContext = require("viewmodels/common/sharding/shardingContext");
import viewModelBase = require("viewmodels/viewModelBase");

class importParent extends viewModelBase {

    context: shardingContext;
    
    view = require("views/database/tasks/importParent.html");

    importDataOptionsUrl = appUrl.forCurrentDatabase().importDataOptionsUrl;

    getView() {
        return this.view;
    }
    
    router: DurandalRootRouter;

    constructor() {
        super();
        
        this.context = new shardingContext("allShards");
        
        // the nav-tabs strip is gone from importParent.html - these child routes only route,
        // they are not navigation entries anymore
        this.router = durandalRouter.createChildRouter()
            .map([
                {
                    route: 'databases/tasks/import/migrateRavenDB',
                    moduleId: this.wrapModuleId(require('viewmodels/database/tasks/migrateRavenDbDatabase')),
                    title: 'Import database from another RavenDB',
                    nav: false,
                    dynamicHash: appUrl.forCurrentDatabase().migrateRavenDbDatabaseUrl,
                    requiredAccess: "DatabaseReadWrite"
                },
                {
                    route: 'databases/tasks/import/csv',
                    moduleId: this.wrapModuleId(require('viewmodels/database/tasks/importCollectionFromCsv')),
                    title: 'Import collection from CSV file',
                    nav: false,
                    dynamicHash: appUrl.forCurrentDatabase().importCollectionFromCsv,
                    requiredAccess: "DatabaseReadWrite"
                },
                {
                    route: 'databases/tasks/import/sql',
                    moduleId: this.wrapModuleId(require('viewmodels/database/tasks/importDatabaseFromSql')),
                    title: 'Import from SQL Database',
                    nav: false,
                    dynamicHash: appUrl.forCurrentDatabase().importDatabaseFromSql,
                    requiredAccess: "DatabaseReadWrite"
                },
                {
                    route: 'databases/tasks/import/migrate',
                    moduleId: this.wrapModuleId(require('viewmodels/database/tasks/migrateDatabase')),
                    title: 'Migrate database',
                    nav: false,
                    dynamicHash: appUrl.forCurrentDatabase().migrateDatabaseUrl,
                    requiredAccess: "DatabaseReadWrite"
                }
            ])
            .buildNavigationModel();
    }
    
    wrapModuleId(item: any) {
        const container = require('viewmodels/common/sharding/shardAwareContainer');
        return new container("both", item, this.context);
    }
}

export = importParent; 

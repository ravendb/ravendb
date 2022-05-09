import appUrl = require("common/appUrl");

import durandalRouter = require("plugins/router");
import shardingContext from "viewmodels/common/sharding/shardingContext";
import viewModelBase from "viewmodels/viewModelBase";

class importParent extends viewModelBase {

    context: shardingContext;
    
    view = require("views/database/tasks/importParent.html");
    
    getView() {
        return this.view;
    }
    
    router: DurandalRootRouter;

    constructor() {
        super();
        
        this.context = new shardingContext("allShards");
        
        this.router = durandalRouter.createChildRouter()
            .map([
                {
                    route: 'databases/tasks/import/file',
                    moduleId: this.wrapModuleId(require("viewmodels/database/tasks/importDatabaseFromFile")),
                    title: 'Import database from file',
                    nav: true,
                    tabName: "From file (.ravendbdump)",
                    dynamicHash: appUrl.forCurrentDatabase().importDatabaseFromFileUrl,
                    requiredAccess: "DatabaseReadWrite"
                },
                {
                    route: 'databases/tasks/import/migrateRavenDB',
                    moduleId: this.wrapModuleId(require('viewmodels/database/tasks/migrateRavenDbDatabase')),
                    title: 'Import database from another RavenDB',
                    tabName: "From RavenDB Server",
                    nav: true,
                    dynamicHash: appUrl.forCurrentDatabase().migrateRavenDbDatabaseUrl,
                    requiredAccess: "DatabaseReadWrite"
                },
                {
                    route: 'databases/tasks/import/csv',
                    moduleId: this.wrapModuleId(require('viewmodels/database/tasks/importCollectionFromCsv')),
                    title: 'Import collection from CSV file',
                    tabName: "From CSV File",
                    nav: true,
                    dynamicHash: appUrl.forCurrentDatabase().importCollectionFromCsv,
                    requiredAccess: "DatabaseReadWrite"
                },
                {
                    route: 'databases/tasks/import/sql',
                    moduleId: this.wrapModuleId(require('viewmodels/database/tasks/importDatabaseFromSql')),
                    title: 'Import from SQL Database',
                    tabName: "From SQL",
                    nav: true,
                    dynamicHash: appUrl.forCurrentDatabase().importDatabaseFromSql,
                    requiredAccess: "DatabaseReadWrite"
                },
                {
                    route: 'databases/tasks/import/migrate',
                    moduleId: this.wrapModuleId(require('viewmodels/database/tasks/migrateDatabase')),
                    title: 'Migrate database',
                    tabName: "From NoSQL",
                    nav: true,
                    dynamicHash: appUrl.forCurrentDatabase().migrateDatabaseUrl,
                    requiredAccess: "DatabaseReadWrite"
                }
            ])
            .buildNavigationModel();
    }
    
    wrapModuleId(item: Function) {
        const container = require('viewmodels/common/sharding/shardAwareContainer');
        return new container("both", item, this.context);
    }
}

export = importParent; 

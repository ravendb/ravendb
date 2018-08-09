import viewModelBase = require("viewmodels/viewModelBase");
import migrateDatabaseCommand = require("commands/database/studio/migrateDatabaseCommand");
import migrateDatabaseModel = require("models/database/tasks/migrateDatabaseModel");
import notificationCenter = require("common/notifications/notificationCenter");
import eventsCollector = require("common/eventsCollector");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");

interface databasesInfo {
    Databases: Array<string>;
}

interface collectionInfo {
    Collections: Array<string>;
    HasGridFS: boolean;
}

class migrateDatabase extends viewModelBase {

    model = new migrateDatabaseModel();

    spinners = {
        getDatabaseNames: ko.observable<boolean>(false),
        getCollectionNames: ko.observable<boolean>(false),
        migration: ko.observable<boolean>(false)
    };

    constructor() {
        super();

        aceEditorBindingHandler.install();

        this.model.migratorFullPath.subscribe(() => this.getDatabases());
        this.model.mongoDbConfiguration.connectionString.subscribe(() => {
            this.getDatabases();
            this.getCollections();
        });
        this.model.cosmosDbConfiguration.azureEndpointUrl.subscribe(() => {
            this.getDatabases();
            this.getCollections();
        });
        this.model.cosmosDbConfiguration.primaryKey.subscribe(() => {
            this.getDatabases();
            this.getCollections();
        });
        this.model.mongoDbConfiguration.databaseName.subscribe(() => this.getCollections());
        this.model.cosmosDbConfiguration.databaseName.subscribe(() => this.getCollections());
    }

    getDatabases() {
        if (!this.isValid(this.model.validationGroupDatabasesCommand)) {
            return;
        }

        this.spinners.getDatabaseNames(true);

        const db = this.activeDatabase();
        const selectMigrationOption = this.model.selectMigrationOption();
        new migrateDatabaseCommand<databasesInfo>(db, this.model.toDto("databases"), true)
            .execute()
            .done((databasesInfo: databasesInfo) => {
                if (selectMigrationOption !== this.model.selectMigrationOption()) {
                    return;
                }

                switch (selectMigrationOption) {
                    case "MongoDB":
                        this.model.mongoDbConfiguration.databaseNames(databasesInfo.Databases);
                        break;
                    case "CosmosDB":
                        this.model.cosmosDbConfiguration.databaseNames(databasesInfo.Databases);
                        break;
                }
            })
            .fail(() => {
                switch (selectMigrationOption) {
                    case "MongoDB":
                        this.model.mongoDbConfiguration.databaseNames([]);
                        this.model.mongoDbConfiguration.setCollections([]);
                        this.model.mongoDbConfiguration.hasGridFs(false);
                        break;
                    case "CosmosDB":
                        this.model.cosmosDbConfiguration.databaseNames([]);
                        this.model.cosmosDbConfiguration.setCollections([]);
                        break;
                }
            })
            .always(() => this.spinners.getDatabaseNames(false));
    }

    getCollections() {
        if (!this.isValid(this.model.validationGroup)) {
            return;
        }

        this.spinners.getCollectionNames(true);

        const db = this.activeDatabase();
        const selectMigrationOption = this.model.selectMigrationOption();
        new migrateDatabaseCommand<collectionInfo>(db, this.model.toDto("collections"), true)
            .execute()
            .done((collectionInfo: collectionInfo) => {
                if (selectMigrationOption !== this.model.selectMigrationOption()) {
                    return;
                }

                switch (selectMigrationOption) {
                    case "MongoDB":
                        this.model.mongoDbConfiguration.setCollections(collectionInfo.Collections);
                        this.model.mongoDbConfiguration.hasGridFs(collectionInfo.HasGridFS);
                        break;
                    case "CosmosDB":
                        this.model.cosmosDbConfiguration.setCollections(collectionInfo.Collections);
                        break;
                }
            })
            .fail(() => {
                switch (selectMigrationOption) {
                    case "MongoDB":
                        this.model.mongoDbConfiguration.setCollections([]);
                        this.model.mongoDbConfiguration.hasGridFs(false);
                        break;
                    case "CosmosDB":
                        this.model.cosmosDbConfiguration.setCollections([]);
                        break;
                }
            })
            .always(() => this.spinners.getCollectionNames(false));
    }
    
    migrateDb() {
        if (!this.isValid(this.model.validationGroup)) {
            return;
        }

        eventsCollector.default.reportEvent("database", "migrate");
        this.spinners.migration(true);

        const db = this.activeDatabase();

        new migrateDatabaseCommand<operationIdDto>(db, this.model.toDto("export"), false)
            .execute()
            .done((operationIdDto: operationIdDto) => {
                const operationId = operationIdDto.OperationId;
                notificationCenter.instance.openDetailsForOperationById(db, operationId);
            })
            .always(() => this.spinners.migration(false));
    }
}

export = migrateDatabase; 

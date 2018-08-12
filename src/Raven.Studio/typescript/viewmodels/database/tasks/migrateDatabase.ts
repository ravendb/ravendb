import viewModelBase = require("viewmodels/viewModelBase");
import migrateDatabaseCommand = require("commands/database/studio/migrateDatabaseCommand");
import migrateDatabaseModel = require("models/database/tasks/migrateDatabaseModel");
import notificationCenter = require("common/notifications/notificationCenter");
import eventsCollector = require("common/eventsCollector");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import popoverUtils = require("common/popoverUtils");

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

    compositionComplete() {
        super.compositionComplete();

        popoverUtils.longWithHover($(".migrator-path small"),
            {
                content: '<strong>Raven.Migrator.exe</strong> can be found in <strong>tools</strong><br /> package (for version v4.x) on <a target="_blank" href="http://ravendb.net/downloads">ravendb.net</a> website'
            });

        popoverUtils.longWithHover($(".migrate-gridfs small"),
            {
                content: 'GridFS attachments will be saved as documents with attachments in <strong>@files</strong> collection.'
            });
    }

    getDatabases() {
        const activeConfiguration = this.model.activeConfiguration();
        if (!this.isValid(this.model.validationGroupDatabasesCommand) || !activeConfiguration) {
            if (!activeConfiguration) {
                activeConfiguration.collectionsToMigrate([]);
            }
            return;
        }

        this.spinners.getDatabaseNames(true);
        const selectMigrationOption = this.model.selectMigrationOption();
        const db = this.activeDatabase();
        
        new migrateDatabaseCommand<databasesInfo>(db, this.model.toDto("databases"), true)
            .execute()
            .done((databasesInfo: databasesInfo) => {
                if (selectMigrationOption !== this.model.selectMigrationOption()) {
                    return;
                }

                activeConfiguration.databaseNames(databasesInfo.Databases);
            })
            .fail(() => {
                activeConfiguration.databaseNames([]);
                activeConfiguration.setCollections([]);
                if (selectMigrationOption === "MongoDB") {
                    this.model.mongoDbConfiguration.hasGridFs(false);
                }
            })
            .always(() => this.spinners.getDatabaseNames(false));
    }

    getCollections() {
        const activeConfiguration = this.model.activeConfiguration();
        if (!this.isValid(this.model.validationGroup) || !activeConfiguration) {
            if (!activeConfiguration) {
                activeConfiguration.collectionsToMigrate([]);
            }
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

                activeConfiguration.setCollections(collectionInfo.Collections);
                if (selectMigrationOption === "MongoDB") {
                    this.model.mongoDbConfiguration.hasGridFs(collectionInfo.HasGridFS);
                }
            })
            .fail(() => {
                activeConfiguration.setCollections([]);
                if (selectMigrationOption === "MongoDB") {
                    this.model.mongoDbConfiguration.hasGridFs(false);
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

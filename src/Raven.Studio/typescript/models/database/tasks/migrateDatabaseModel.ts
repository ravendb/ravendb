/// <reference path="../../../../typings/tsd.d.ts"/>

type migrationOptions = "none" | "MongoDB" | "CosmosDB";
type availableNoSqlCommands = "databases" | "collections" | "export";

interface IMongoDbMigrationConfiguration extends IAbstractMigrationConfiguration {
    ConnectionString: string;
    MigrateGridFS: boolean;
}

interface ICosmosDbMigrationConfiguration extends IAbstractMigrationConfiguration {
    AzureEndpointUrl: string;
    PrimaryKey: string;
}

interface IAbstractMigrationConfiguration {
    Command: string;
    DatabaseName: string;
    ConsoleExport: boolean;
    ExportFilePath: string;
    CollectionsToMigrate: ICollection[];
}

interface ICollection {
    Name: string;
    NewName: string;
}

class collection {
    name = ko.observable<string>();
    newName = ko.observable<string>();
    migrateCollection = ko.observable<boolean>();

    constructor(name: string) {
        this.name(name);
        this.newName(name);
        this.migrateCollection(true);
    }

    toDto(): ICollection {
        return {
            Name: this.name(),
            NewName: this.newName()
        }
    }
}

abstract class noSqlMigrationModel {
    databaseName = ko.observable<string>();
    collectionsToMigrate = ko.observableArray<collection>([]);
    databaseNames = ko.observableArray<string>([]);
    migrateAllCollections = ko.observable<boolean>(true);
    hasCollectionsToMigrate: KnockoutComputed<boolean>;
    migratingCollectionsText: KnockoutComputed<string>;
    actualCollectionsToMigrate: KnockoutComputed<collection[]>;

    constructor() {
        this.hasCollectionsToMigrate = ko.pureComputed(() => this.collectionsToMigrate().length > 0);

        this.actualCollectionsToMigrate = ko.pureComputed(() => this.collectionsToMigrate().filter(x => x.migrateCollection()));

        this.migratingCollectionsText = ko.pureComputed(() => {
            const collectionsToMigrate = this.actualCollectionsToMigrate().length;
            if (collectionsToMigrate === 0) {
                return "No collections were selected for migration";
            }

            const pluralize = collectionsToMigrate > 1 ? "s" : "";
            return `Migarting ${collectionsToMigrate} collection${pluralize}`;
        });
    }

    createDatabaseNamesAutoCompleter() {
        return ko.pureComputed(() => {
            const options = this.databaseNames();
            let key = this.databaseName();

            if (key) {
                key = key.toLowerCase();
                return options.filter(x => x.toLowerCase().includes(key));
            }

            return options;
        });
    }

    selectDatabaseName(databaseName: string) {
        this.databaseName(databaseName);
    }

    setCollections(collections: string[]) {
        const collectionsToMigrate = collections.map(c => new collection(c));
        this.collectionsToMigrate(collectionsToMigrate);
        this.migrateAllCollections(true);
    }

    toDto(command: string): IAbstractMigrationConfiguration {
        const collectionsToMigrate = this.migrateAllCollections() ? null : this.actualCollectionsToMigrate().map(x => x.toDto());
        return {
            DatabaseName: this.databaseName(),
            Command: command,
            CollectionsToMigrate: collectionsToMigrate,
            ConsoleExport: true,
            ExportFilePath: null
        };
    }
}

class mongoDbMigrationModel extends noSqlMigrationModel{
    connectionString = ko.observable<string>();
    migrateGridFs = ko.observable<boolean>(false);
    hasGridFs = ko.observable<boolean>();

    toDto(command: string): IMongoDbMigrationConfiguration {
        const dto = super.toDto(command) as IMongoDbMigrationConfiguration;
        dto.ConnectionString = this.connectionString();
        dto.MigrateGridFS = this.migrateGridFs();
        return dto;
    }
}

class cosmosDbMigrationModel extends noSqlMigrationModel {
    azureEndpointUrl = ko.observable<string>();
    primaryKey = ko.observable<string>();

    toDto(command: string): ICosmosDbMigrationConfiguration {
        const dto = super.toDto(command) as ICosmosDbMigrationConfiguration;
        dto.AzureEndpointUrl = this.azureEndpointUrl();
        dto.PrimaryKey = this.primaryKey();
        return dto;
    }
}

class migrateDatabaseModel {
    selectMigrationOption = ko.observable<migrationOptions>("none");
    fullPathToMigrator = ko.observable<string>();
    transformScript = ko.observable<string>();

    mongoDbConfiguration = new mongoDbMigrationModel();
    cosmosDbConfiguration = new cosmosDbMigrationModel();

    activeConfiguration: KnockoutComputed<noSqlMigrationModel>;
    showMongoDbOptions: KnockoutComputed<boolean>;
    showCosmosDbOptions: KnockoutComputed<boolean>;

    validationGroup: KnockoutValidationGroup;
    validationGroupDatabasesCommand: KnockoutValidationGroup;

    constructor() {
        this.initObservables();
        this.initValidation();
    }

    private initObservables() {
        this.activeConfiguration = ko.pureComputed(() => {
            switch (this.selectMigrationOption()) {
                case "MongoDB":
                    return this.mongoDbConfiguration;
                case "CosmosDB":
                    return this.cosmosDbConfiguration;
                default:
                    return null;
            }
        });

        this.showMongoDbOptions = ko.pureComputed(() => this.selectMigrationOption() === "MongoDB");
        this.showCosmosDbOptions = ko.pureComputed(() => this.selectMigrationOption() === "CosmosDB");
    }
    
    private initValidation() {
        this.fullPathToMigrator.extend({
            required: true
        });

        this.selectMigrationOption.extend({
            validation: [
                {
                    validator: (value: migrationOptions) => value !== "none",
                    message: "Please choose a database source"
                }
            ]
        });

        this.mongoDbConfiguration.databaseName.extend({
            required: {
                onlyIf: () => this.showMongoDbOptions()
            }
        });

        this.mongoDbConfiguration.connectionString.extend({
            required: {
                onlyIf: () => this.showMongoDbOptions()
            },
            validation: [
                {
                    validator: (value: string) => {
                        if (!this.showMongoDbOptions()) {
                            return true;
                        }
                        const prefix = "mongodb://";
                        return value && value.toLowerCase().startsWith(prefix) &&
                            value.length > prefix.length + 1;
                    },
                    message: "Invalid MongoDB connection string"
                }
            ]
        });

        this.cosmosDbConfiguration.databaseName.extend({
            required: {
                onlyIf: () => this.showCosmosDbOptions()
            }
        });

        this.cosmosDbConfiguration.azureEndpointUrl.extend({
            required: {
                onlyIf: () => this.showCosmosDbOptions()
            },
            validUrl: true
        });

        this.cosmosDbConfiguration.primaryKey.extend({
            required: {
                onlyIf: () => this.showCosmosDbOptions()
            }
        });

        this.validationGroup = ko.validatedObservable({
            fullPathToMigrator: this.fullPathToMigrator,
            selectMigrationOption: this.selectMigrationOption,
            mongoDbDatabaseName: this.mongoDbConfiguration.databaseName,
            connectionString: this.mongoDbConfiguration.connectionString,
            cosmosDbDatabaseName: this.cosmosDbConfiguration.databaseName,
            azureEndpointUrl: this.cosmosDbConfiguration.azureEndpointUrl,
            primaryKey: this.cosmosDbConfiguration.primaryKey
        });

        this.validationGroupDatabasesCommand = ko.validatedObservable({
            fullPathToMigrator: this.fullPathToMigrator,
            selectMigrationOption: this.selectMigrationOption,
            connectionString: this.mongoDbConfiguration.connectionString,
            azureEndpointUrl: this.cosmosDbConfiguration.azureEndpointUrl,
            primaryKey: this.cosmosDbConfiguration.primaryKey
        });
    }

    toDto(command: availableNoSqlCommands): Raven.Server.Smuggler.Migration.MigrationConfiguration {
        const activeConfiguration = this.activeConfiguration();
        if (!activeConfiguration) {
            return null;
        }

        const inputConfiguration = activeConfiguration.toDto(command);

        const type = this.selectMigrationOption().toLowerCase();
        return {
            DatabaseTypeName: type,
            FullPathToMigrator: this.fullPathToMigrator(),
            InputConfiguration: inputConfiguration,
            TransformScript: this.transformScript()
        };
    }
}

export = migrateDatabaseModel;

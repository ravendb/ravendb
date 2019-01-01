/// <reference path="../../../../typings/tsd.d.ts"/>

type migrationOptions = "MongoDB" | "CosmosDB";

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
    selectedCollectionsCount: KnockoutComputed<number>;

    constructor() {
        this.hasCollectionsToMigrate = ko.pureComputed(() => this.collectionsToMigrate().length > 0);
        this.selectedCollectionsCount = ko.pureComputed(() => this.collectionsToMigrate().filter(x => x.migrateCollection()).length);
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

    toDto(): IAbstractMigrationConfiguration {
        const collectionsToMigrate = this.migrateAllCollections() 
            ? null 
            : this.collectionsToMigrate().filter(x => x.migrateCollection()).map(x => x.toDto());
        return {
            DatabaseName: this.databaseName(),
            Command: null, // will be filled in later
            CollectionsToMigrate: collectionsToMigrate,
            ConsoleExport: true,
            ExportFilePath: null
        };
    }
}

class mongoDbMigrationModel extends noSqlMigrationModel {
    connectionString = ko.observable<string>();
    migrateGridFs = ko.observable<boolean>(false);
    hasGridFs = ko.observable<boolean>();

    toDto(): IMongoDbMigrationConfiguration {
        const dto = super.toDto() as IMongoDbMigrationConfiguration;
        dto.ConnectionString = this.connectionString();
        dto.MigrateGridFS = this.migrateGridFs();
        return dto;
    }
}

class cosmosDbMigrationModel extends noSqlMigrationModel {
    azureEndpointUrl = ko.observable<string>();
    primaryKey = ko.observable<string>();

    toDto(): ICosmosDbMigrationConfiguration {
        const dto = super.toDto() as ICosmosDbMigrationConfiguration;
        dto.AzureEndpointUrl = this.azureEndpointUrl();
        dto.PrimaryKey = this.primaryKey();
        return dto;
    }
}

class migrateDatabaseModel {
    selectMigrationOption = ko.observable<migrationOptions>();
    migratorFullPath = ko.observable<string>();
    showTransformScript = ko.observable<boolean>(false);
    transformScript = ko.observable<string>();

    mongoDbConfiguration = new mongoDbMigrationModel();
    cosmosDbConfiguration = new cosmosDbMigrationModel();

    activeConfiguration: KnockoutComputed<noSqlMigrationModel>;
    showMongoDbOptions: KnockoutComputed<boolean>;
    showCosmosDbOptions: KnockoutComputed<boolean>;

    validationGroup: KnockoutValidationGroup;
    validationGroupDatabaseNames: KnockoutValidationGroup;

    constructor() {
        this.initObservables();
        this.initValidation();

        this.showTransformScript.subscribe(v => {
            if (v) {
                this.transformScript(
                    "this.collection = this['@metadata']['@collection'];\r\n" +
                    "// current object is available under 'this' variable\r\n" +
                    "// @change-vector, @id, @last-modified metadata fields are not available");
            } else {
                this.transformScript("");
            }
        });
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
        this.migratorFullPath.extend({
            required: true
        });

        this.selectMigrationOption.extend({
            validation: [
                {
                    validator: (value: migrationOptions) => !!value,
                    message: "Please choose source"
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
            migratorFullPath: this.migratorFullPath,
            selectMigrationOption: this.selectMigrationOption,
            mongoDbDatabaseName: this.mongoDbConfiguration.databaseName,
            connectionString: this.mongoDbConfiguration.connectionString,
            cosmosDbDatabaseName: this.cosmosDbConfiguration.databaseName,
            azureEndpointUrl: this.cosmosDbConfiguration.azureEndpointUrl,
            primaryKey: this.cosmosDbConfiguration.primaryKey
        });

        this.validationGroupDatabaseNames = ko.validatedObservable({
            migratorFullPath: this.migratorFullPath,
            selectMigrationOption: this.selectMigrationOption,
            connectionString: this.mongoDbConfiguration.connectionString,
            azureEndpointUrl: this.cosmosDbConfiguration.azureEndpointUrl,
            primaryKey: this.cosmosDbConfiguration.primaryKey
        });
    }

    toDto(): Raven.Server.Smuggler.Migration.MigrationConfiguration {
        const activeConfiguration = this.activeConfiguration();
        if (!activeConfiguration) {
            return null;
        }

        const inputConfiguration = activeConfiguration.toDto();

        const type = this.selectMigrationOption().toLowerCase();
        return {
            DatabaseTypeName: type,
            MigratorFullPath: this.migratorFullPath(),
            InputConfiguration: inputConfiguration,
            TransformScript: this.transformScript()
        };
    }
}

export = migrateDatabaseModel;

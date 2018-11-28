/// <reference path="../../../../../typings/tsd.d.ts"/>
import abstractSqlTable = require("models/database/tasks/sql/abstractSqlTable");
import rootSqlTable = require("models/database/tasks/sql/rootSqlTable");

import sqlColumn = require("models/database/tasks/sql/sqlColumn");
import sqlReference = require("models/database/tasks/sql/sqlReference");

class sqlMigration {
    
    static possibleProviders = ["MsSQL", "MySQL", "NpgSQL", "Oracle"] as Array<Raven.Server.SqlMigration.MigrationProvider>;
    
    databaseType = ko.observable<Raven.Server.SqlMigration.MigrationProvider>("MsSQL");
    binaryToAttachment = ko.observable<boolean>(true);
    batchSize = ko.observable<number>(1000);
    
    testImport = ko.observable<boolean>(false);
    maxDocumentsToImportPerTable = ko.observable<number>(1000);

    advanced = {
        usePascalCase: ko.observable<boolean>(true),
        trimSuffix: ko.observable<boolean>(true),
        suffixToTrim: ko.observable<string>("_id"),
        detectManyToMany: ko.observable<boolean>(true),
        includeUnsupported: ko.observable<boolean>(false)
    };
    
    static sqlServerConnectionString = ko.observable<string>();
    static mysqlConnectionString = ko.observable<string>();
    static npgsqlConnectionString = ko.observable<string>();
    static oracleConnectionString = ko.observable<string>();

    sqlServer = {
        connectionString: sqlMigration.sqlServerConnectionString
    };
    
    sqlServerValidationGroup: KnockoutValidationGroup;
    
    mySql = {
        connectionString: sqlMigration.mysqlConnectionString
    };
    
    connectionStringOverride = ko.observable<string>();
    
    mySqlValidationGroup: KnockoutValidationGroup;

    npgSql = {
        connectionString: sqlMigration.npgsqlConnectionString
    };

    npgSqlValidationGroup: KnockoutValidationGroup;

    oracle = {
        connectionString: sqlMigration.oracleConnectionString
    };

    oracleValidationGroup: KnockoutValidationGroup;

    tables = ko.observableArray<rootSqlTable>([]);
    
    dbSchema: Raven.Server.SqlMigration.Schema.DatabaseSchema;
    
    collectionNameTransformationFunc: (name: string) => string;
    propertyNameTransformationFunc: (name: string) => string;
    
    constructor() {
        this.initObservables();
        this.initTransformationFunctions();
        this.initValidation();
    }
    
    private initObservables() {
        this.advanced.usePascalCase.subscribe(() => this.initTransformationFunctions());
        this.advanced.trimSuffix.subscribe(() => this.initTransformationFunctions());
        this.advanced.suffixToTrim.subscribe(() => this.initTransformationFunctions());
    }

    initValidation() {
        this.sqlServer.connectionString.extend({
                required: true
            });
        
        this.mySql.connectionString.extend({
            required: true
        });

        this.npgSql.connectionString.extend({
            required: true
        });

        this.oracle.connectionString.extend({
            required: true
        });

        this.sqlServerValidationGroup = ko.validatedObservable({
            connectionString: this.sqlServer.connectionString,
            batchSize: this.batchSize,
            maxDocumentsToImportPerTable: this.maxDocumentsToImportPerTable
        });

        this.mySqlValidationGroup = ko.validatedObservable({
            connectionString: this.mySql.connectionString,
            batchSize: this.batchSize,
            maxDocumentsToImportPerTable: this.maxDocumentsToImportPerTable
        });

        this.npgSqlValidationGroup = ko.validatedObservable({
            connectionString: this.npgSql.connectionString,
            batchSize: this.batchSize,
            maxDocumentsToImportPerTable: this.maxDocumentsToImportPerTable
        });

        this.oracleValidationGroup = ko.validatedObservable({
            connectionString: this.oracle.connectionString,
            batchSize: this.batchSize,
            maxDocumentsToImportPerTable: this.maxDocumentsToImportPerTable
        });

        this.batchSize.extend({
            required: true
        });
        
        this.maxDocumentsToImportPerTable.extend({
            required: {
                onlyIf: () => this.testImport()
            }
        })
    }
    
    private initTransformationFunctions() {
        const suffix = this.advanced.suffixToTrim();
        const pascal = (input: string) => _.upperFirst(_.camelCase(input));
        const removeSuffix = (input: string) => input.endsWith(suffix) ? input.slice(0, -suffix.length) : input;
        const identity = (input: string) => input;
        
        this.collectionNameTransformationFunc = this.advanced.usePascalCase() ? pascal : identity;
        if (this.advanced.usePascalCase()) {
            this.propertyNameTransformationFunc = this.advanced.trimSuffix() ? _.flow(removeSuffix, pascal) : pascal;
        } else {
            this.propertyNameTransformationFunc = this.advanced.trimSuffix() ? removeSuffix : identity;
        }
    }

    labelForProvider(type: Raven.Server.SqlMigration.MigrationProvider) {
        switch (type) {
            case "MsSQL":
                return "Microsoft SQL Server (System.Data.SqlClient)";
            case "MySQL":
                return "MySQL Server (MySql.Data.MySqlClient)";
            case "NpgSQL":
                return "PostgreSQL Server (Npgsql)";
            case "Oracle":
                return "Oracle Database (Oracle.ManagedDataAccess.Client)";
            default:
                return type;
        }
    }
    
    private findSpecialColumnNames(dbSchema: Raven.Server.SqlMigration.Schema.DatabaseSchema, tableSchema: string, tableName: string): string[] {
        const result = [] as Array<string>;
        const mainSchema = dbSchema.Tables.find(x => x.Schema === tableSchema && x.TableName === tableName);
        
        result.push(...mainSchema.PrimaryKeyColumns);
        
        dbSchema.Tables.forEach(fkCandidate => {
            fkCandidate.References.filter(x => x.Schema === tableSchema && x.Table === x.Table).forEach(tableReference => {
                result.push(...tableReference.Columns);
            });
        });
        
        return result;
    }
    
    onSchemaUpdated(dbSchema: Raven.Server.SqlMigration.Schema.DatabaseSchema, defaultSetup = true) {
        this.dbSchema = dbSchema;
        
        const mapping = _.map(dbSchema.Tables, tableDto => {
            const table = new rootSqlTable();
            
            table.tableName = tableDto.TableName;
            table.tableSchema = tableDto.Schema;
            table.collectionName(tableDto.TableName);
            table.query(tableDto.DefaultQuery);
            const columns = tableDto.Columns.map(columnDto => new sqlColumn(columnDto));
            const primaryKeyColumns = columns.filter(c => _.includes(tableDto.PrimaryKeyColumns, c.sqlName));
            const specialColumnNames = this.findSpecialColumnNames(dbSchema, tableDto.Schema, tableDto.TableName);
            const primaryKeyColumnNames = primaryKeyColumns.map(x => x.sqlName);
            if (this.advanced.includeUnsupported()) {
                table.documentColumns(columns.filter(c => !_.includes(specialColumnNames, c.sqlName) && !_.includes(primaryKeyColumnNames, c.sqlName)));
            } else {
                const unsupportedColumnNames = columns.filter(c => c.type == "Unsupported").map(c => c.sqlName);
                table.documentColumns(columns.filter(c => !_.includes(specialColumnNames, c.sqlName) && !_.includes(primaryKeyColumnNames, c.sqlName)
                    && !_.includes(unsupportedColumnNames, c.sqlName)));
            }

            table.primaryKeyColumns(primaryKeyColumns);
            
            return table;
        });
        
        // insert references
        _.map(dbSchema.Tables, tableDto => {
            const sourceTable = sqlMigration.findRootTable(mapping, tableDto.Schema, tableDto.TableName);
            tableDto.References.forEach(referenceDto => {
                const targetTable = sqlMigration.findRootTable(mapping, referenceDto.Schema, referenceDto.Table);
                
                const oneToMany = new sqlReference(targetTable, sourceTable, referenceDto.Columns, "OneToMany");
                oneToMany.skip();
                sourceTable.references.push(oneToMany);
                
                const manyToOne = new sqlReference(sourceTable, targetTable, referenceDto.Columns, "ManyToOne");
                if (defaultSetup) {
                    manyToOne.link(sourceTable);    
                } else {
                    manyToOne.skip();
                }
                
                targetTable.references.push(manyToOne);
            });
        });
        
        this.updateNames(mapping);
        
        this.tables(mapping);
        
        if (defaultSetup && this.advanced.detectManyToMany()) {
            this.detectManyToMany();
        }
    }
    
    findReverseReference(reference: sqlReference) {
        const targetTable = this.findRootTable(reference.targetTable.tableSchema, reference.targetTable.tableName);
        return targetTable.references()
            .find(r => r.type !== reference.type 
                && _.isEqual(r.joinColumns, reference.joinColumns) 
                && r.targetTable.tableSchema === reference.sourceTable.tableSchema 
                && r.targetTable.tableName === reference.sourceTable.tableName);
    }
    
    private updateNames(mapping: Array<rootSqlTable>) {
        mapping.forEach(rootTable => {
            rootTable.collectionName(this.collectionNameTransformationFunc(rootTable.collectionName()));
            
            this.updatePropertyNames(rootTable);
        });
    }
    
    updatePropertyNames(table: abstractSqlTable) {
        table.documentColumns().forEach(column => {
            column.propertyName(this.propertyNameTransformationFunc(column.sqlName));
        });
        
        table.references()
            .filter(x => x.type === "ManyToOne")
            .forEach(ref => {
                ref.name(ref.joinColumns.map(this.propertyNameTransformationFunc).join("And"));
            });
        
        _.forEach(_.groupBy(table.references()
            .filter(x => x.type === 'OneToMany'), x => x.targetTable.tableName), (refs, name) => {
            
            const basicName = this.propertyNameTransformationFunc(name);
            
            refs.forEach((ref: sqlReference, idx: number) => {
                ref.name(refs.length > 1 ? basicName + (idx + 1) : basicName)
            });
        });
    }
    
    static findRootTable(tables: Array<rootSqlTable>, tableSchema: string,  tableName: string) {
        return tables.find(x => x.tableName === tableName && x.tableSchema === tableSchema);
    }
    
    findRootTable(tableSchema: string,  tableName: string) {
        return sqlMigration.findRootTable(this.tables(), tableSchema, tableName);
    }
    
    findLinksToTable(table: abstractSqlTable): Array<sqlReference> {
        return _.flatMap(this.tables().filter(x => x.checked()), t => t.findLinksToTable(table));
    }

    getFactoryName() {
        switch (this.databaseType()) {
            case "MySQL":
                return "MySql.Data.MySqlClient";
            case "MsSQL":
                return "System.Data.SqlClient";
            case "NpgSQL":
                return "Npgsql";
            case "Oracle":
                return "Oracle.ManagedDataAccess.Client";
            default:
                throw new Error(`Can't get factory name: Database type - ${this.databaseType} - is not supported.`);
        }
    }
    
    getConnectionString() {
        if (this.connectionStringOverride()) {
            // used when we import configuration file
            return this.connectionStringOverride();
        }
        
        switch (this.databaseType()) {
            case "MySQL":
                return this.mySql.connectionString();
                
            case "MsSQL":
                return this.sqlServer.connectionString();

            case "NpgSQL":
                return this.npgSql.connectionString();

            case "Oracle":
                return this.oracle.connectionString();

            default:
                throw new Error(`Database type - ${this.databaseType} - is not supported`);
        }
    }
    
    toSourceDto(): Raven.Server.SqlMigration.Model.SourceSqlDatabase {
        return {
            ConnectionString: this.getConnectionString(),
            Provider: this.databaseType()
        }
    }
    
    toDto(): Raven.Server.SqlMigration.Model.MigrationRequest {
        const binaryToAttachment = this.binaryToAttachment();
        return {
            Source: this.toSourceDto(),
            Settings: {
                BatchSize: this.batchSize(),
                MaxRowsPerTable: this.testImport() ? this.maxDocumentsToImportPerTable() : undefined,
                
                Collections: this.tables()
                    .filter(x => x.checked())
                    .map(x => x.toDto(binaryToAttachment))
            }
        } as Raven.Server.SqlMigration.Model.MigrationRequest;
    }
    
    toCollectionsMappingDto(): Array<Raven.Server.SqlMigration.Model.CollectionNamesMapping> {
        return this.tables()
            .filter(x => x.checked())
            .map(table => {
                return {
                    CollectionName: table.collectionName(),
                    TableSchema: table.tableSchema,
                    TableName: table.tableName
                } as Raven.Server.SqlMigration.Model.CollectionNamesMapping;
            });
    }
    
    advancedSettingsDto(): sqlMigrationAdvancedSettingsDto {
        return {
            UsePascalCase: this.advanced.usePascalCase(),
            DetectManyToMany: this.advanced.detectManyToMany(),
            TrimSuffix: this.advanced.trimSuffix(),
            SuffixToTrim: this.advanced.suffixToTrim()
        }
    }
    
    loadAdvancedSettings(dto: sqlMigrationAdvancedSettingsDto) {
        this.advanced.usePascalCase(dto.UsePascalCase);
        this.advanced.trimSuffix(dto.TrimSuffix);
        this.advanced.suffixToTrim(dto.SuffixToTrim);
        this.advanced.detectManyToMany(dto.DetectManyToMany);
    }

    getValidationGroup(): KnockoutValidationGroup {
        switch (this.databaseType()) {
            case "MySQL":
                return this.mySqlValidationGroup;

            case "MsSQL":
                return this.sqlServerValidationGroup;

            case "NpgSQL":
                return this.npgSqlValidationGroup;

            case "Oracle":
                return this.oracleValidationGroup;

            default:
                throw new Error(`Database type - ${this.databaseType()} - is not supported`);
        }
    }
    
    private escape(inputString: string) {
        return inputString ? inputString.replace("'", "''") : "";
    }
    
    getSelectedTablesCount() {
        return _.sumBy(this.tables(), t => t.checked() ? 1 : 0);
    }

    private detectManyToMany() {
        const manyToManyTables = this.tables().filter(t => t.isManyToMany());

        manyToManyTables.forEach(table => {
            table.references().forEach(reference => {
                reference.action("skip");

                const reverseReference = this.findReverseReference(reference);
                this.onEmbedTable(reverseReference);
                
                const reverseTable = reverseReference.sourceTable as rootSqlTable;
                reverseTable.transformResults(true);
                const firstInnerReference = reverseReference.effectiveInnerTable().references()[0];
                const innerPropertyName = firstInnerReference.name();
                firstInnerReference.link(firstInnerReference.targetTable);
                
                const script = "// flatten many-to-many relationship\r\n"
                    + "this." + reverseReference.name() + " = this." + reverseReference.name() + ".map(x => x." + innerPropertyName + ");\r\n";
                
                reverseTable.patchScript(script);
            });

            table.checked(false);
        });
    }
    
    onEmbedTable(reference: sqlReference) {
        const tableToEmbed = this.findRootTable(reference.targetTable.tableSchema, reference.targetTable.tableName);
        const innerTable = tableToEmbed.cloneForEmbed(reference);
        
        // setup initial state of cloned object
        innerTable.references().forEach(innerReference => {
            if (innerReference.action() === "link") {
                const tableToLink = this.findRootTable(innerReference.targetTable.tableSchema, innerReference.targetTable.tableName);
                innerReference.effectiveLinkTable(tableToLink);
            }
        });
        
        this.updatePropertyNames(innerTable);
        innerTable.removeBackReference(reference);
        
        reference.embed(innerTable);
    }
    
    setAllLinksToSkip() {
        this.tables().forEach(table => table.setAllLinksToSkip());
    }
    
    applyConfiguration(config: Raven.Server.SqlMigration.Model.MigrationRequest) {
        const source = config.Source;
        
        this.databaseType(source.Provider);
        this.connectionStringOverride(source.ConnectionString);
        
        const settings = config.Settings;
        this.batchSize(settings.BatchSize);
        this.testImport(!!settings.MaxRowsPerTable);
        this.maxDocumentsToImportPerTable(this.testImport() ? settings.MaxRowsPerTable : 1000);
        
        settings.Collections.forEach(collection => {
            const rootTable = this.findRootTable(collection.SourceTableSchema, collection.SourceTableName);
            this.applyConfigurationToCollection(rootTable, collection, "root");
        });
        
        this.tables().forEach(table => {
            const matchedTable = settings.Collections.find(x => x.SourceTableSchema === table.tableSchema && x.SourceTableName === table.tableName);
            if (!matchedTable) {
                table.checked(false);
            }
        })
    }
    
    private applyConfigurationToCollection(table: abstractSqlTable, collection: Raven.Server.SqlMigration.Model.AbstractCollection, collectionType: "root" | "embed") {
        if (collectionType === "root") {
            const rootCollection = collection as Raven.Server.SqlMigration.Model.RootCollection;
            const rootTable = table as rootSqlTable;
            
            if (rootCollection.SourceTableQuery) {
                rootTable.customizeQuery(true);
                rootTable.query(rootCollection.SourceTableQuery);
            }
            
            if (rootCollection.Patch) {
                rootTable.transformResults(true);
                rootTable.patchScript(rootCollection.Patch);
            }
            
            rootTable.collectionName(rootCollection.Name);
        }
        
        const withReferences = collection as Raven.Server.SqlMigration.Model.CollectionWithReferences;
            
        withReferences.NestedCollections.forEach(nested => {
            const reference = table.findReference(nested);
            reference.name(nested.Name);
            this.onEmbedTable(reference);
            
            this.applyConfigurationToCollection(reference.effectiveInnerTable(), nested, "embed");
        });
        
        withReferences.LinkedCollections.forEach(linked => {
            const reference = table.findReference(linked);
            reference.link(reference.targetTable);
            reference.name(linked.Name);
        });
        
        Object.keys(collection.ColumnsMapping).forEach(sqlName => {
            const column = table.documentColumns().find(x => x.sqlName === sqlName);
            column.propertyName(collection.ColumnsMapping[sqlName]);
        });
    }
}

export = sqlMigration;

/// <reference path="../../../../../typings/tsd.d.ts"/>
import abstractSqlTable = require("models/database/tasks/sql/abstractSqlTable");
import rootSqlTable = require("models/database/tasks/sql/rootSqlTable");

import sqlColumn = require("models/database/tasks/sql/sqlColumn");
import sqlReference = require("models/database/tasks/sql/sqlReference");

class sqlMigration {
    
    static possibleProviders = ["MsSQL", "MySQL"] as Array<Raven.Server.SqlMigration.MigrationProvider>;
    
    databaseType = ko.observable<Raven.Server.SqlMigration.MigrationProvider>("MySQL");
    sourceDatabaseName = ko.observable<string>("northwind");
    binaryToAttachment = ko.observable<boolean>(true);
    batchSize = ko.observable<number>(1000);
    
    testImport = ko.observable<boolean>(false);
    maxDocumentsToImportPerTable = ko.observable<number>(1000);
    
    sqlServer = {
        connectionString: ko.observable<string>()
    };
    
    sqlServerValidationGroup: KnockoutValidationGroup;
    
    mySql = {
        server: ko.observable<string>("127.0.0.1"),
        username: ko.observable<string>("root"),
        password: ko.observable<string>() 
    };
    
    mySqlValidationGroup: KnockoutValidationGroup;
    
    tables = ko.observableArray<rootSqlTable>([]); 
    
    constructor() {    
        this.initValidation();   
    }

    initValidation() {
        
        this.sqlServer.connectionString.extend({
                required: true
            });
        
        this.mySql.server.extend({
            required: true
        });

        this.mySql.username.extend({
            required: true
        });
        
        this.sourceDatabaseName.extend({
            required: true
        });
        
        this.sqlServerValidationGroup = ko.validatedObservable({
            connectionString: this.sqlServer.connectionString,
            sourceDatabaseName: this.sourceDatabaseName,
            batchSize: this.batchSize,
            maxDocumentsToImportPerTable: this.maxDocumentsToImportPerTable
        });

        this.mySqlValidationGroup = ko.validatedObservable({
            server: this.mySql.server,
            username: this.mySql.username,            
            password: this.mySql.password,
            sourceDatabaseName: this.sourceDatabaseName,
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

    labelForProvider(type: Raven.Server.SqlMigration.MigrationProvider) {
        switch (type) {
            case "MsSQL":
                return "Microsoft SQL Server";
            case "MySQL":
                return "MySQL Server";
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
    
    onSchemaUpdated(dbSchema: Raven.Server.SqlMigration.Schema.DatabaseSchema) {
        const mapping = _.map(dbSchema.Tables, tableDto => {
            const table = new rootSqlTable();
            
            table.tableName = tableDto.TableName;
            table.tableSchema = tableDto.Schema;
            table.collectionName(tableDto.TableName);
            const columns = tableDto.Columns.map(columnDto => new sqlColumn(columnDto));
            const primaryKeyColumns = columns.filter(c => _.includes(tableDto.PrimaryKeyColumns, c.sqlName));
            const specialColumnNames = this.findSpecialColumnNames(dbSchema, tableDto.Schema, tableDto.TableName);
            const primaryKeyColumnNames = primaryKeyColumns.map(x => x.sqlName);
            
            table.documentColumns(columns.filter(c => !_.includes(specialColumnNames, c.sqlName) && !_.includes(primaryKeyColumnNames, c.sqlName)));
            table.primaryKeyColumns(primaryKeyColumns);
            
            return table;
        });
        
        // insert references
        _.map(dbSchema.Tables, tableDto => {
            const sourceTable = sqlMigration.findRootTable(mapping, tableDto.Schema, tableDto.TableName);
            tableDto.References.forEach(referenceDto => {
                const targetTable = sqlMigration.findRootTable(mapping, referenceDto.Schema, referenceDto.Table);
                
                const oneToMany = new sqlReference(targetTable, sourceTable, referenceDto.Columns, "OneToMany");
                sourceTable.references.push(oneToMany);
                
                const manyToOne = new sqlReference(sourceTable, targetTable, referenceDto.Columns, "ManyToOne");
                targetTable.references.push(manyToOne);
                manyToOne.effectiveLinkTable(sourceTable);
            });
        });
        
        this.updateNames(mapping);
        
        this.tables(mapping);
    }
    
    private updateNames(mapping: Array<rootSqlTable>) {
        
        const collectionNameFunc = (input: string) => _.upperFirst(_.camelCase(input));
        const propertyNameFunc = (input: string) => _.upperFirst(_.camelCase(input));
        
        mapping.forEach(rootTable => {
            rootTable.collectionName(collectionNameFunc(rootTable.collectionName()));
            
            sqlMigration.updatePropertyNames(rootTable, propertyNameFunc);
        });
    }
    
    static updatePropertyNames(table: abstractSqlTable, transformFunction: (input: string) => string) {
        table.documentColumns().forEach(column => {
            column.propertyName(transformFunction(column.sqlName));
        });
        
        table.references()
            .filter(x => x.type === "ManyToOne")
            .forEach(ref => {
                ref.name(ref.joinColumns.map(transformFunction).join("And"));
            });
        
        _.forEach(_.groupBy(table.references()
            .filter(x => x.type === 'OneToMany'), x => x.targetTable.tableName), (refs, name) => {
            
            const basicName = transformFunction(name);
            
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
    
    getConnectionString() {
        switch (this.databaseType()) {
            case "MySQL":
                let mySQLConnectionString = `server='${this.escape(this.mySql.server())}';` +
                                            `uid='${this.escape(this.mySql.username())}'\;` +
                                            `database='${this.escape(this.sourceDatabaseName())}'`;
                if (this.mySql.password()) {
                    mySQLConnectionString += `\;pwd='${this.escape(this.mySql.password())}'`;
                } 
                return mySQLConnectionString;
                
            case "MsSQL":
                // Append initial catalog. For now we don't take it from the connection string.
                return `${this.sqlServer.connectionString()}\;Initial Catalog='${this.escape(this.sourceDatabaseName())}'`;
                
            default:
                throw new Error(`Database type - ${this.databaseType} - is not supported`);
        }
    }
    
    toDto(): Raven.Server.SqlMigration.Model.MigrationRequest {
        return {
            Source: {
                ConnectionString: this.getConnectionString(),
                Provider: this.databaseType()
            },
            Settings: {
                BatchSize: this.batchSize(),
                BinaryToAttachment: this.binaryToAttachment(),
                MaxRowsPerTable: this.testImport() ? this.maxDocumentsToImportPerTable() : undefined,
                
                Collections: this.tables()
                    .filter(x => x.checked())
                    .map(x => x.toDto())
            }
        } as Raven.Server.SqlMigration.Model.MigrationRequest;
    }

    getValidationGroup(): KnockoutValidationGroup {
        switch (this.databaseType()) {
            case "MySQL":
                return this.mySqlValidationGroup;

            case "MsSQL":
                return this.sqlServerValidationGroup;

            default:
                throw new Error(`Database type - ${this.databaseType()} - is not supported`);
        }
    }
    
    private escape(inputString: string) {
        return inputString.replace("'", "''");
    }
    
    getSelectedTablesCount() {
        return _.sumBy(this.tables(), t => t.checked() ? 1 : 0);
    }
}

export = sqlMigration;

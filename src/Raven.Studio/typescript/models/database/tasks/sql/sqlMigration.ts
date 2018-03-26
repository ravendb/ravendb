/// <reference path="../../../../../typings/tsd.d.ts"/>
import sqlTable = require("models/database/tasks/sql/sqlTable");
import sqlColumn = require("models/database/tasks/sql/sqlColumn");
import sqlReference = require("models/database/tasks/sql/sqlReference");

class sqlMigration {
    
    static possibleProviders = ["MsSQL", "MySQL"] as Array<Raven.Server.SqlMigration.MigrationProvider>;
    
    databaseType = ko.observable<Raven.Server.SqlMigration.MigrationProvider>("MsSQL");
    sourceDatabaseName = ko.observable<string>();
    binaryToAttachment = ko.observable<boolean>(true);
    batchSize = ko.observable<number>(1000);
    
    sqlServer = {
        connectionString: ko.observable<string>()
    };
    
    sqlServerValidationGroup: KnockoutValidationGroup;
    
    mySql = {
        server: ko.observable<string>(),
        username: ko.observable<string>(),
        password: ko.observable<string>() 
    };
    
    mySqlValidationGroup: KnockoutValidationGroup;
    
    tables = ko.observableArray<sqlTable>([]); 
    
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
            sourceDatabaseName: this.sourceDatabaseName
        });

        this.mySqlValidationGroup = ko.validatedObservable({
            server: this.mySql.server,
            username: this.mySql.username,            
            password: this.mySql.password,
            sourceDatabaseName: this.sourceDatabaseName
        });
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
    
    onSchemaUpdated(dbSchema: Raven.Server.SqlMigration.Schema.DatabaseSchema) {
        const mapping = _.map(dbSchema.Tables, tableDto => {
            const table = new sqlTable();
            
            table.tableName = tableDto.TableName;
            table.tableSchema = tableDto.Schema;
            table.customCollection(tableDto.TableName);
            const columns = tableDto.Columns.map(columnDto => new sqlColumn(columnDto));
            const primaryKeyColumns = columns.filter(c => _.includes(tableDto.PrimaryKeyColumns, c.name));
            const documentColumns = _.without(columns, ...primaryKeyColumns);
            table.columns(documentColumns);
            table.primaryKeyColumns(primaryKeyColumns);
            
            return table;
        });
        
        //TODO: remove special columns
        
        // insert references
        _.map(dbSchema.Tables, tableDto => {
            const sourceTable = mapping.find(x => x.tableName === tableDto.TableName && x.tableSchema === tableDto.Schema);
            tableDto.References.forEach(referenceDto => {
                const targetTable = mapping.find(x => x.tableName === referenceDto.Table && x.tableSchema === referenceDto.Schema);
                
                const oneToMany = new sqlReference(targetTable, referenceDto.Columns, "OneToMany");
                sourceTable.references.push(oneToMany);
                
                const manyToOne = new sqlReference(sourceTable, referenceDto.Columns, "ManyToOne");
                targetTable.references.push(manyToOne);
            });
        });
        
        
        this.tables(mapping);
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
                let msSQLConnectionString = `${this.sqlServer.connectionString()}\;Initial Catalog='${this.escape(this.sourceDatabaseName())}'`;
                return msSQLConnectionString;
                
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
}

export = sqlMigration;

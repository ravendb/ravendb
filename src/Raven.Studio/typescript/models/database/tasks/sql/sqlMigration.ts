/// <reference path="../../../../../typings/tsd.d.ts"/>
import sqlTable = require("models/database/tasks/sql/sqlTable");
import sqlColumn = require("./sqlColumn");

class sqlMigration {
    
    static possibleProviders = ["MsSQL", "MySQL"] as Array<Raven.Server.SqlMigration.MigrationProvider>;
    
    databaseType = ko.observable<Raven.Server.SqlMigration.MigrationProvider>("MsSQL");
    sourceDatabaseName = ko.observable<string>();
    
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
        
        //TODO: remember password in MySQL is not required        
        //TODO: use proper validation group based on database type 
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
        const mapping = _.map(dbSchema.Tables, (tableDto, tableName) => {
            const table = new sqlTable();
            
            table.name(tableName);
            table.columns(tableDto.Columns.map(columnDto => new sqlColumn(columnDto)));
            
            return table;
        });
        
        this.tables(mapping);
    }
    
    getConnectionString() {
        //TODO: generate based on collected settings
        // for mySQL it will something like: - remember about escaping 
        // server=127.0.0.1;uid=root;pwd=123;database=ABC
        return "Data Source=MARCIN-WIN\\INSERTNEXO;Integrated Security=True;Initial Catalog=SqlTest_bfebf597-9916-499b-a2c8-45aa702f77aa";
    }
}

export = sqlMigration;

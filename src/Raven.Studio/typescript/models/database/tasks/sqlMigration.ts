/// <reference path="../../../../typings/tsd.d.ts"/>

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
    
    constructor() {
        //TODO: add validation etc, 
        //TODO: remember password in MySQL is not required
        //TODO: use proper validation group based on database type 
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
    
    getConnectionString() {
        //TODO: generate based on collected settings
        // for mySQL it will something like: - remember about escaping 
        // server=127.0.0.1;uid=root;pwd=123;database=ABC
        return "Data Source=MARCIN-WIN\\INSERTNEXO;Integrated Security=True;Initial Catalog=SqlTest_bfebf597-9916-499b-a2c8-45aa702f77aa";
    }
    
}


export = sqlMigration;

/// <reference path="../../../../typings/tsd.d.ts"/>
import connectionStringModel = require("models/database/settings/connectionStringModel");
import database = require("models/resources/database");
import testSqlConnectionStringCommand = require("commands/database/cluster/testSqlConnectionStringCommand");
import saveConnectionStringCommand = require("commands/database/settings/saveConnectionStringCommand");

class connectionStringSqlEtlModel extends connectionStringModel {
    
    connectionString = ko.observable<string>();     
    
    validationGroup: KnockoutValidationGroup;
    testConnectionValidationGroup: KnockoutValidationGroup;  

    constructor(dto: Raven.Client.ServerWide.ETL.SqlConnectionString, isNew: boolean, tasks: string[]) {
        super(isNew, tasks);
        
        this.update(dto);
        this.initValidation();
    }

    update(dto: Raven.Client.ServerWide.ETL.SqlConnectionString) {
        super.update(dto);
        
        this.connectionStringName(dto.Name); 
        this.connectionString(dto.ConnectionString); 
    }

    initValidation() {
        super.initValidation();
        
        this.connectionStringName.extend({
            required: true
        });

        this.connectionString.extend({
            required: true
        });

        this.validationGroup = ko.validatedObservable({
            connectionStringName: this.connectionStringName,
            connectionString: this.connectionString
        });

        this.testConnectionValidationGroup = ko.validatedObservable({
            connectionString: this.connectionString
        })
    }

    static empty(): connectionStringSqlEtlModel {
        return new connectionStringSqlEtlModel({
            Type: "Sql",
            Name: "",
            ConnectionString: ""
        } as Raven.Client.ServerWide.ETL.SqlConnectionString, true, []);
    }
    
    toDto() {
        return {
            Type: "Sql",
            Name: this.connectionStringName(),
            ConnectionString: this.connectionString()
        };
    }
    
    testConnection(db: database) : JQueryPromise<Raven.Server.Web.System.NodeConnectionTestResult> {
        return new testSqlConnectionStringCommand(db, this.connectionString())
            .execute();
    }

    saveConnectionString(db: database) : JQueryPromise<void> {
        return new saveConnectionStringCommand(db, this)
            .execute();
    }
}

export = connectionStringSqlEtlModel;

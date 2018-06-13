/// <reference path="../../../../typings/tsd.d.ts"/>
import connectionStringModel = require("models/database/settings/connectionStringModel");
import database = require("models/resources/database");
import testSqlConnectionStringCommand = require("commands/database/cluster/testSqlConnectionStringCommand");
import saveConnectionStringCommand = require("commands/database/settings/saveConnectionStringCommand");
import jsonUtil = require("common/jsonUtil");

class connectionStringSqlEtlModel extends connectionStringModel {

    static sqlProviders = [
        "System.Data.SqlClient",
        "Npgsql",
        "Oracle.ManagedDataAccess.Client",
        "MySql.Data.MySqlClient"
    ] as Array<string>;
    
    connectionString = ko.observable<string>();
    factoryName = ko.observable<string>();
    
    validationGroup: KnockoutValidationGroup;
    testConnectionValidationGroup: KnockoutValidationGroup;  
    
    dirtyFlag: () => DirtyFlag;

    constructor(dto: Raven.Client.Documents.Operations.ETL.SQL.SqlConnectionString, isNew: boolean, tasks: { taskName: string; taskId: number }[]) {
        super(isNew, tasks);
        
        this.update(dto);
        this.initValidation();
        
        this.dirtyFlag = new ko.DirtyFlag([           
            this.connectionString,
            this.connectionStringName
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }

    update(dto: Raven.Client.Documents.Operations.ETL.SQL.SqlConnectionString) {
        super.update(dto);
        
        this.connectionStringName(dto.Name); 
        this.connectionString(dto.ConnectionString); 
        this.factoryName(dto.FactoryName);
    }

    initValidation() {
        super.initValidation();
        
        this.connectionStringName.extend({
            required: true
        });

        this.connectionString.extend({
            required: true
        });
        
        this.factoryName.extend({
            required: true
        });

        this.validationGroup = ko.validatedObservable({
            connectionStringName: this.connectionStringName,
            connectionString: this.connectionString,
            factoryName: this.factoryName
        });

        this.testConnectionValidationGroup = ko.validatedObservable({
            connectionString: this.connectionString,
            factoryName: this.factoryName
        })
    }

    static empty(): connectionStringSqlEtlModel {
        return new connectionStringSqlEtlModel({
            Type: "Sql",
            FactoryName: connectionStringSqlEtlModel.sqlProviders[0],
            Name: "",
            ConnectionString: ""
        } as Raven.Client.Documents.Operations.ETL.SQL.SqlConnectionString, true, []);
    }
    
    toDto() {
        return {
            Type: "Sql",
            Name: this.connectionStringName(),
            FactoryName: this.factoryName(),
            ConnectionString: this.connectionString()
        };
    }
    
    testConnection(db: database) : JQueryPromise<Raven.Server.Web.System.NodeConnectionTestResult> {
        return new testSqlConnectionStringCommand(db, this.connectionString(), this.factoryName())
            .execute();
    }

    saveConnectionString(db: database) : JQueryPromise<void> {
        return new saveConnectionStringCommand(db, this)
            .execute();
    }
}

export = connectionStringSqlEtlModel;

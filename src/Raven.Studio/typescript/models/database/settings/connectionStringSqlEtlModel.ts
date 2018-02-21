/// <reference path="../../../../typings/tsd.d.ts"/>
import connectionStringModel = require("models/database/settings/connectionStringModel");
import database = require("models/resources/database");
import testSqlConnectionStringCommand = require("commands/database/cluster/testSqlConnectionStringCommand");
import saveConnectionStringCommand = require("commands/database/settings/saveConnectionStringCommand");
import jsonUtil = require("common/jsonUtil");

class connectionStringSqlEtlModel extends connectionStringModel {
    
    connectionString = ko.observable<string>();     
    
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
        } as Raven.Client.Documents.Operations.ETL.SQL.SqlConnectionString, true, []);
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

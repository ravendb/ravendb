/// <reference path="../../../../typings/tsd.d.ts"/>
import connectionStringModel = require("models/database/settings/connectionStringModel");
import database = require("models/resources/database");
import saveConnectionStringCommand_OLD = require("commands/database/settings/saveConnectionStringCommand_OLD");
import jsonUtil = require("common/jsonUtil");
import testSnowflakeConnectionStringCommand from "commands/database/cluster/testSnowflakeConnectionStringCommand";

class connectionStringSnowflakeEtlModel extends connectionStringModel {

    connectionString = ko.observable<string>();
    
    validationGroup: KnockoutValidationGroup;
    testConnectionValidationGroup: KnockoutValidationGroup;
    
    dirtyFlag: () => DirtyFlag;

    constructor(dto: Raven.Client.Documents.Operations.ETL.Snowflake.SnowflakeConnectionString, isNew: boolean, tasks: { taskName: string; taskId: number }[]) {
        super(isNew, tasks);
        
        this.update(dto);
        this.initValidation();
        
        this.dirtyFlag = new ko.DirtyFlag([
            this.connectionString,
            this.connectionStringName
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }

    update(dto: Raven.Client.Documents.Operations.ETL.Snowflake.SnowflakeConnectionString): void {
        super.update(dto);
        
        this.connectionStringName(dto.Name); 
        this.connectionString(dto.ConnectionString); 
    }

    initValidation(): void {
        super.initValidation();
        
        this.connectionStringName.extend({
            required: true
        });

        this.connectionString.extend({
            required: true
        });

        this.validationGroup = ko.validatedObservable({
            connectionStringName: this.connectionStringName,
            connectionString: this.connectionString,
        });

        this.testConnectionValidationGroup = ko.validatedObservable({
            connectionString: this.connectionString,
        })
    }

    static empty(): connectionStringSnowflakeEtlModel {
        return new connectionStringSnowflakeEtlModel({
            Type: "Snowflake",
            Name: "",
            ConnectionString: ""
        }, true, []);
    }
    
    toDto(): Raven.Client.Documents.Operations.ETL.Snowflake.SnowflakeConnectionString {
        return {
            Type: "Snowflake",
            Name: this.connectionStringName(),
            ConnectionString: this.connectionString()
        };
    }
    
    testConnection(db: database): JQueryPromise<Raven.Server.Web.System.NodeConnectionTestResult> {
        return new testSnowflakeConnectionStringCommand(db, this.connectionString())
            .execute();
    }

    saveConnectionString(db: database): JQueryPromise<void> {
        return new saveConnectionStringCommand_OLD(db, this)
            .execute();
    }
}

export = connectionStringSnowflakeEtlModel;

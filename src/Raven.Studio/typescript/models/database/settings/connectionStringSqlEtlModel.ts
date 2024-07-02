/// <reference path="../../../../typings/tsd.d.ts"/>
import connectionStringModel = require("models/database/settings/connectionStringModel");
import database = require("models/resources/database");
import testSqlConnectionStringCommand = require("commands/database/cluster/testSqlConnectionStringCommand");
import saveConnectionStringCommand_OLD = require("commands/database/settings/saveConnectionStringCommand_OLD");
import jsonUtil = require("common/jsonUtil");
import sqlConnectionStringSyntax = require("common/helpers/database/sqlConnectionStringSyntax");

class connectionStringSqlEtlModel extends connectionStringModel {

    static sqlProviders: Array<valueAndLabelItem<string, string> & { tooltip: string }> = [
        { value: "Microsoft.Data.SqlClient", label: "Microsoft SQL Server", tooltip: sqlConnectionStringSyntax.mssqlSyntax },
        { value: "MySqlConnector.MySqlConnectorFactory", label: "MySQL Server", tooltip: sqlConnectionStringSyntax.mysqlSyntax },
        { value: "Npgsql", label: "PostgreSQL", tooltip: sqlConnectionStringSyntax.npgsqlSyntax },
        { value: "Oracle.ManagedDataAccess.Client", label: "Oracle Database", tooltip: sqlConnectionStringSyntax.oracleSyntax },
        { value: "MySql.Data.MySqlClient", label: "DEPRECATED: MySQL Server", tooltip: sqlConnectionStringSyntax.mysqlSyntax },
        { value: "System.Data.SqlClient", label: "DEPRECATED: Microsoft SQL Server", tooltip: sqlConnectionStringSyntax.mssqlSyntax },
    ];
    
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
            this.factoryName,
            this.connectionStringName
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }

    update(dto: Raven.Client.Documents.Operations.ETL.SQL.SqlConnectionString): void {
        super.update(dto);
        
        this.connectionStringName(dto.Name); 
        this.connectionString(dto.ConnectionString); 
        this.factoryName(dto.FactoryName);
    }

    initValidation(): void {
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
    
    simpleNameFor(factoryName: string): string {
        const provider = connectionStringSqlEtlModel.sqlProviders.find(x => x.value === factoryName);
        return provider ? provider.label : null;
    }
    
    fullNameFor(factoryName: string): string {
        const provider = connectionStringSqlEtlModel.sqlProviders.find(x => x.value === factoryName);
        return provider ? `${provider.label} (${provider.value})` : null;
    }
    
    syntaxHtml(): string {
        const provider = connectionStringSqlEtlModel.sqlProviders.find(x => x.value === this.factoryName());
        return provider ? provider.tooltip : null;
    }

    static empty(): connectionStringSqlEtlModel {
        return new connectionStringSqlEtlModel({
            Type: "Sql",
            FactoryName: null,
            Name: "",
            ConnectionString: ""
        }, true, []);
    }
    
    toDto(): Raven.Client.Documents.Operations.ETL.SQL.SqlConnectionString {
        return {
            Type: "Sql",
            Name: this.connectionStringName(),
            FactoryName: this.factoryName(),
            ConnectionString: this.connectionString()
        };
    }
    
    testConnection(db: database): JQueryPromise<Raven.Server.Web.System.NodeConnectionTestResult> {
        return new testSqlConnectionStringCommand(db, this.connectionString(), this.factoryName())
            .execute();
    }

    saveConnectionString(db: database): JQueryPromise<void> {
        return new saveConnectionStringCommand_OLD(db, this)
            .execute();
    }
    
    factoryPlaceHolder(factoryName: KnockoutObservable<string>) {
        return ko.pureComputed(() => {
            const simpleName = this.simpleNameFor(factoryName());
            
            if (!factoryName()) {
                return "Enter connection string";
            }
            
            return  `Enter the complete connection string for the ${simpleName}`;
        });
    }
}

export = connectionStringSqlEtlModel;

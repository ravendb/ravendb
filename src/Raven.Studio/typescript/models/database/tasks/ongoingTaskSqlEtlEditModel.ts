/// <reference path="../../../../typings/tsd.d.ts"/>
import ongoingTaskEditModel = require("models/database/tasks/ongoingTaskEditModel");
import ongoingTaskSqlEtlTransformationModel = require("models/database/tasks/ongoingTaskSqlEtlTransformationModel");
import ongoingTaskSqlEtlTableModel = require("models/database/tasks/ongoingTaskSqlEtlTableModel");

class ongoingTaskSqlEtlEditModel extends ongoingTaskEditModel {
    connectionStringName = ko.observable<string>();

    allowEtlOnNonEncryptedChannel = ko.observable<boolean>(false);
    
    parameterizedDeletes = ko.observable<boolean>(false);
    forceRecompileQuery = ko.observable<boolean>(false);
    tableQuotation = ko.observable<boolean>(false);
    commandTimeout = ko.observable<number>();
    
    transformationScripts = ko.observableArray<ongoingTaskSqlEtlTransformationModel>([]);
    sqlTables = ko.observableArray<ongoingTaskSqlEtlTableModel>([]);
    
    validationGroup: KnockoutValidationGroup;
    enterTestModeValidationGroup: KnockoutValidationGroup;
    dirtyFlag: () => DirtyFlag;
   
    constructor(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskSqlEtlDetails) {
        super();

        this.update(dto);
        this.initializeObservables();
        this.initializeValidation();
    }
    
    protected initializeObservables() {
        super.initializeObservables();
        
        this.dirtyFlag = new ko.DirtyFlag([
            this.taskName,
            this.connectionStringName,
            this.mentorNode,
            this.manualChooseMentor,
            this.parameterizedDeletes,
            this.forceRecompileQuery,
            this.tableQuotation,
            this.commandTimeout,
            this.allowEtlOnNonEncryptedChannel
        ])
    }
    
    initializeValidation() {
        this.initializeMentorValidation();

        this.connectionStringName.extend({
            required: true
        });

        this.commandTimeout.extend({
            number: true
        });
        
        this.sqlTables.extend({
            validation: [
                {
                    validator: () => this.sqlTables().length > 0,
                    message: "SQL table is Not defined"
                }
            ]
        });

        this.transformationScripts.extend({
            validation: [
                {
                    validator: () => this.transformationScripts().length > 0,
                    message: "Transformation Script is Not defined"
                }
            ]
        });

        this.validationGroup = ko.validatedObservable({
            connectionStringName: this.connectionStringName,
            sqlTables: this.sqlTables,
            transformationScripts: this.transformationScripts,
            mentorNode: this.mentorNode,
            commandTimeout: this.commandTimeout
        });
        
        this.enterTestModeValidationGroup = ko.validatedObservable({
            connectionStringName: this.connectionStringName,
            sqlTables: this.sqlTables
        });
    }

    update(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskSqlEtlDetails) {
        super.update(dto);

        if (dto.Configuration) {
            this.connectionStringName(dto.Configuration.ConnectionStringName);
            this.parameterizedDeletes(dto.Configuration.ParameterizeDeletes);
            this.forceRecompileQuery(dto.Configuration.ForceQueryRecompile);
            this.tableQuotation(dto.Configuration.QuoteTables);
            this.commandTimeout(dto.Configuration.CommandTimeout);
            
            this.manualChooseMentor(!!dto.Configuration.MentorNode);
            
            this.transformationScripts(dto.Configuration.Transforms.map(x => new ongoingTaskSqlEtlTransformationModel(x, false, false)));
            this.sqlTables(dto.Configuration.SqlTables.map(x => new ongoingTaskSqlEtlTableModel(x, false)));            
        }        
    }

    toDto(): Raven.Client.Documents.Operations.ETL.SQL.SqlEtlConfiguration {
        return {
            TaskId: this.taskId,
            Name: this.taskName(),
            EtlType: "Sql",
            ConnectionStringName: this.connectionStringName(),
            AllowEtlOnNonEncryptedChannel: this.allowEtlOnNonEncryptedChannel(),
            Disabled: false,                     
            MentorNode: this.manualChooseMentor() ? this.mentorNode() : undefined, 
            FactoryName: "System.Data.SqlClient",
            ForceQueryRecompile: this.forceRecompileQuery(),
            ParameterizeDeletes: this.parameterizedDeletes(),
            CommandTimeout: this.commandTimeout() || null,
            QuoteTables: this.tableQuotation(),
            Transforms: this.transformationScripts().map(x => x.toDto()),
            SqlTables: this.sqlTables().map(x => x.toDto())     
        
        } as Raven.Client.Documents.Operations.ETL.SQL.SqlEtlConfiguration;
    }
    
    static empty(): ongoingTaskSqlEtlEditModel {
        return new ongoingTaskSqlEtlEditModel(
            {
                TaskName: "", 
                TaskType: "SqlEtl",
                TaskState: "Enabled",               
                TaskConnectionStatus: "Active",                
                Configuration: {                
                     ForceQueryRecompile: false,
                     ParameterizeDeletes: false,
                     QuoteTables: false,
                     Transforms: [],
                     SqlTables: []
                }
            } as Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskSqlEtlDetails);
    }
}

export = ongoingTaskSqlEtlEditModel;

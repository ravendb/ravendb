/// <reference path="../../../../typings/tsd.d.ts"/>
import ongoingTaskEditModel = require("models/database/tasks/ongoingTaskEditModel");
import ongoingTaskSqlEtlTransformationModel = require("models/database/tasks/ongoingTaskSqlEtlTransformationModel");
import ongoingTaskSqlEtlTableModel = require("models/database/tasks/ongoingTaskSqlEtlTableModel");

class ongoingTaskSqlEtlEditModel extends ongoingTaskEditModel {
    connectionStringName = ko.observable<string>();
    
    parameterizedDeletes = ko.observable<boolean>(false);
    forceRecompileQuery = ko.observable<boolean>(false);
    tableQuotation = ko.observable<boolean>(false);
    
    transformationScripts = ko.observableArray<ongoingTaskSqlEtlTransformationModel>([]);
    sqlTables = ko.observableArray<ongoingTaskSqlEtlTableModel>([]);     
   
    constructor(dto: Raven.Client.ServerWide.Operations.OngoingTaskSqlEtlDetails) {
        super();

        this.update(dto);
        super.initializeObservables();       
    }

    update(dto: Raven.Client.ServerWide.Operations.OngoingTaskSqlEtlDetails) {
        super.update(dto);

        if (dto.Configuration) {
            this.connectionStringName(dto.Configuration.ConnectionStringName);
            this.parameterizedDeletes(dto.Configuration.ParameterizeDeletes);
            this.forceRecompileQuery(dto.Configuration.ForceQueryRecompile);
            this.tableQuotation(dto.Configuration.QuoteTables);
            
            this.transformationScripts(dto.Configuration.Transforms.map(x => new ongoingTaskSqlEtlTransformationModel(x, false)));
            this.sqlTables(dto.Configuration.SqlTables.map(x => new ongoingTaskSqlEtlTableModel(x, false)));            
        }        
    }

    toDto(): Raven.Client.ServerWide.ETL.SqlEtlConfiguration {
        const transformations = this.transformationScripts().map(x => {
            return {
                Name: x.name(),
                Script: x.script(),
                Collections: [x.collection()],
                ApplyToAllDocuments: false,
                Disabled: false,
                HasLoadAttachment: false
            } as Raven.Client.ServerWide.ETL.Transformation;
        });

        const sqlTables = this.sqlTables().map(x => {
            return {
               DocumentIdColumn: x.primaryKey(),
               TableName: x.tableName(),
               InsertOnlyMode: x.insertOnlyMode()               
            } as Raven.Client.ServerWide.ETL.SqlEtlTable;
        });

        return {
            TaskId: this.taskId,
            Name: this.taskName(),
            EtlType: "Sql",
            ConnectionStringName: this.connectionStringName(),
            AllowEtlOnNonEncryptedChannel: false,
            Disabled: false,                     
            MentorNode: null,
            FactoryName: "System.Data.SqlClient",
            ForceQueryRecompile: this.forceRecompileQuery(),
            ParameterizeDeletes: this.parameterizedDeletes(),
            QuoteTables: this.tableQuotation(),
            Transforms: transformations,
            SqlTables: sqlTables     
        
        } as Raven.Client.ServerWide.ETL.SqlEtlConfiguration;
    }


    static empty(): ongoingTaskSqlEtlEditModel {
        return new ongoingTaskSqlEtlEditModel(
            {
                TaskId: null,
                TaskName: "",
                TaskType: "SqlEtl",
                TaskState: "Enabled",
                ResponsibleNode: null,
                TaskConnectionStatus: "Active",                
                Configuration: {
                    EtlType: "Sql",                    
                    AllowEtlOnNonEncryptedChannel: false,
                    ConnectionStringName: "",
                    Disabled: false,                    
                    Name: "",
                    TaskId: null,
                    MentorNode: null,
                    FactoryName: "System.Data.SqlClient",
                    ForceQueryRecompile: false,
                    ParameterizeDeletes: false,
                    QuoteTables: false,
                    Transforms: [],
                    SqlTables: []
                },
                Error: null,
            });
    }
}

export = ongoingTaskSqlEtlEditModel;

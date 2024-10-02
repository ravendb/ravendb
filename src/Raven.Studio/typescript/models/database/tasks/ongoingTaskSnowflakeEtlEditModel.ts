/// <reference path="../../../../typings/tsd.d.ts"/>
import ongoingTaskEditModel = require("models/database/tasks/ongoingTaskEditModel");
import ongoingTaskSnowflakeEtlTransformationModel
    from "models/database/tasks/ongoingTaskSnowflakeEtlTransformationModel";
import ongoingTaskSnowflakeEtlTableModel from "models/database/tasks/ongoingTaskSnowflakeEtlTableModel";

class ongoingTaskSnowflakeEtlEditModel extends ongoingTaskEditModel {
    connectionStringName = ko.observable<string>();

    allowEtlOnNonEncryptedChannel = ko.observable<boolean>(false);
    
    commandTimeout = ko.observable<number>();
    
    transformationScripts = ko.observableArray<ongoingTaskSnowflakeEtlTransformationModel>([]);
    snowflakeTables = ko.observableArray<ongoingTaskSnowflakeEtlTableModel>([]);
    
    validationGroup: KnockoutValidationGroup;
    enterTestModeValidationGroup: KnockoutValidationGroup;
    dirtyFlag: () => DirtyFlag;
    
    get studioTaskType(): StudioTaskType {
        return "SnowflakeEtl";
    }

    get destinationType(): TaskDestinationType {
        return "Table";
    }
    
    constructor(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskSnowflakeEtl) {
        super();

        this.update(dto);
        this.initializeObservables();
        this.initializeValidation();
    }
    
    protected initializeObservables() {
        super.initializeObservables();
        
        this.dirtyFlag = new ko.DirtyFlag([
            this.taskName,
            this.taskState,
            this.connectionStringName,
            this.mentorNode,
            this.pinMentorNode,
            this.manualChooseMentor,
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
            digit: true
        });
        
        this.snowflakeTables.extend({
            validation: [
                {
                    validator: () => this.snowflakeTables().length > 0,
                    message: "Snowflake table is Not defined"
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
            snowflakeTables: this.snowflakeTables,
            transformationScripts: this.transformationScripts,
            mentorNode: this.mentorNode,
            commandTimeout: this.commandTimeout
        });
        
        this.enterTestModeValidationGroup = ko.validatedObservable({
            connectionStringName: this.connectionStringName,
            snowflakeTables: this.snowflakeTables
        });
    }

    update(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskSnowflakeEtl) {
        super.update(dto);

        if (dto.Configuration) {
            this.connectionStringName(dto.Configuration.ConnectionStringName);
            this.allowEtlOnNonEncryptedChannel(dto.Configuration.AllowEtlOnNonEncryptedChannel);
            this.commandTimeout(dto.Configuration.CommandTimeout);
            
            this.manualChooseMentor(!!dto.Configuration.MentorNode);
            this.pinMentorNode(dto.Configuration.PinToMentorNode);
            this.mentorNode(dto.Configuration.MentorNode);
            
            this.transformationScripts(dto.Configuration.Transforms.map(x => new ongoingTaskSnowflakeEtlTransformationModel(x, false, false)));
            this.snowflakeTables(dto.Configuration.SnowflakeTables.map(x => new ongoingTaskSnowflakeEtlTableModel(x, false)));
        }        
    }

    toDto(): Raven.Client.Documents.Operations.ETL.Snowflake.SnowflakeEtlConfiguration {
        return {
            TaskId: this.taskId,
            Name: this.taskName(),
            EtlType: "Snowflake",
            ConnectionStringName: this.connectionStringName(),
            AllowEtlOnNonEncryptedChannel: this.allowEtlOnNonEncryptedChannel(),
            Disabled: this.taskState() === "Disabled",
            MentorNode: this.manualChooseMentor() ? this.mentorNode() : undefined,
            PinToMentorNode: this.pinMentorNode(),
            CommandTimeout: this.commandTimeout() || null,
            Transforms: this.transformationScripts().map(x => x.toDto()),
            SnowflakeTables: this.snowflakeTables().map(x => x.toDto())
        };
    }
    
    static empty(): ongoingTaskSnowflakeEtlEditModel {
        return new ongoingTaskSnowflakeEtlEditModel(
            {
                TaskName: "",
                TaskType: "SnowflakeEtl",
                TaskState: "Enabled",
                TaskConnectionStatus: "Active",
                Configuration: {
                     Transforms: [],
                     SnowflakeTables: []
                }
            } as Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskSnowflakeEtl);
    }

    hasAdvancedOptionsDefined(): boolean {
        return !!this.commandTimeout()
    }
}

export = ongoingTaskSnowflakeEtlEditModel;

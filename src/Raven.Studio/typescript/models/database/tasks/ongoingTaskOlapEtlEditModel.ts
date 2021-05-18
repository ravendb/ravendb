/// <reference path="../../../../typings/tsd.d.ts"/>
import ongoingTaskEditModel = require("models/database/tasks/ongoingTaskEditModel");
import ongoingTaskOlapEtlTransformationModel = require("models/database/tasks/ongoingTaskOlapEtlTransformationModel");
import ongoingTaskOlapEtlTableModel = require("models/database/tasks/ongoingTaskOlapEtlTableModel");
import cronEditor = require("viewmodels/common/cronEditor");

class ongoingTaskOlapEtlEditModel extends ongoingTaskEditModel {
    connectionStringName = ko.observable<string>();
    
    customPartition = ko.observable<string>();
    customPartitionEnabled = ko.observable<boolean>(false);
        
    runFrequency = ko.observable<string>();
    runFrequencyCronEditor = ko.observable<cronEditor>();
    
    transformationScripts = ko.observableArray<ongoingTaskOlapEtlTransformationModel>([]);
    olapTables = ko.observableArray<ongoingTaskOlapEtlTableModel>([]);
    
    validationGroup: KnockoutValidationGroup;
    dirtyFlag: () => DirtyFlag;

    static readonly defaultRunFrequency = "0 * * * *"; // every hour
   
    constructor(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskOlapEtlDetails) {
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
            this.customPartition,
            this.customPartitionEnabled,
            this.runFrequency
        ])
    }
    
    initializeValidation() {
        this.initializeMentorValidation();

        this.connectionStringName.extend({
            required: true
        });

        this.transformationScripts.extend({
            validation: [
                {
                    validator: () => this.transformationScripts().length > 0,
                    message: "Transformation Script is Not defined"
                }
            ]
        });

        this.customPartition.extend({
            required: {
                onlyIf: () => this.customPartitionEnabled()
            }
        });

        this.validationGroup = ko.validatedObservable({
            connectionStringName: this.connectionStringName,
            olapTables: this.olapTables,
            transformationScripts: this.transformationScripts,
            mentorNode: this.mentorNode,
            customPartition: this.customPartition,
            runFrequency: this.runFrequency
        });
    }

    update(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskOlapEtlDetails) {
        super.update(dto);
        
        const configuration = dto.Configuration;
        if (configuration) {
            this.connectionStringName(configuration.ConnectionStringName);
            this.manualChooseMentor(!!configuration.MentorNode);
            
            this.customPartition(configuration.CustomPartitionValue);
            this.customPartitionEnabled(!!configuration.CustomPartitionValue);
                       
            this.runFrequency(configuration.RunFrequency || ongoingTaskOlapEtlEditModel.defaultRunFrequency);
            this.runFrequencyCronEditor(new cronEditor(this.runFrequency));
            
            if (configuration.Transforms) {
                this.transformationScripts(configuration.Transforms.map(x => new ongoingTaskOlapEtlTransformationModel(x, false, false)));
            }
            
            if (configuration.OlapTables) {
                this.olapTables(configuration.OlapTables.map(x => new ongoingTaskOlapEtlTableModel(x, false)));
            }
        }
        else {
            this.runFrequencyCronEditor(new cronEditor(ko.observable(ongoingTaskOlapEtlEditModel.defaultRunFrequency)));
        }
    }

    toDto(): Raven.Client.Documents.Operations.ETL.OLAP.OlapEtlConfiguration {
        return {
            TaskId: this.taskId,
            Name: this.taskName(),
            EtlType: "Olap",
            ConnectionStringName: this.connectionStringName(),
            AllowEtlOnNonEncryptedChannel: true,
            Disabled: false,
            MentorNode: this.manualChooseMentor() ? this.mentorNode() : undefined,
            Transforms: this.transformationScripts().map(x => x.toDto()),
            CustomPartitionValue: this.customPartitionEnabled() ? this.customPartition() : null,
            RunFrequency: this.runFrequency(),
            OlapTables: this.olapTables().map(x => x.toDto())
        } as Raven.Client.Documents.Operations.ETL.OLAP.OlapEtlConfiguration;
        
    }
    
    static empty(): ongoingTaskOlapEtlEditModel {
        return new ongoingTaskOlapEtlEditModel(
            {
                TaskName: "",
                TaskType: "OlapEtl",
                TaskState: "Enabled",
                TaskConnectionStatus: "Active",
                Configuration: {
                    CustomPartitionValue: "",
                    RunFrequency: this.defaultRunFrequency,
                    Transforms: [],
                    OlapTables: []
                }
            } as Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskOlapEtlDetails);
       }
}

export = ongoingTaskOlapEtlEditModel;

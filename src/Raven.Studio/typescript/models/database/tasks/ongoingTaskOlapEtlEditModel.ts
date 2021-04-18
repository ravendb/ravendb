/// <reference path="../../../../typings/tsd.d.ts"/>
import ongoingTaskEditModel = require("models/database/tasks/ongoingTaskEditModel");
import ongoingTaskOlapEtlTransformationModel = require("models/database/tasks/ongoingTaskOlapEtlTransformationModel");
import ongoingTaskOlapEtlTableModel = require("models/database/tasks/ongoingTaskOlapEtlTableModel");
import cronEditor = require("viewmodels/common/cronEditor");

class ongoingTaskOlapEtlEditModel extends ongoingTaskEditModel {
    connectionStringName = ko.observable<string>();
    
    customPrefix = ko.observable<string>();
    customPrefixEnabled = ko.observable<boolean>(false);
    
    keepFilesOnDisk = ko.observable<boolean>(false);
        
    runFrequency = ko.observable<string>();
    runFrequencyEnabled = ko.observable<boolean>(false);
    runFrequencyCronEditor = ko.observable<cronEditor>();
    
    transformationScripts = ko.observableArray<ongoingTaskOlapEtlTransformationModel>([]);
    olapTables = ko.observableArray<ongoingTaskOlapEtlTableModel>([]);
    
    validationGroup: KnockoutValidationGroup;
    dirtyFlag: () => DirtyFlag;

    static readonly defaultRunFrequency = "0 2 * * *";
   
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
            this.customPrefix,
            this.customPrefixEnabled,
            this.keepFilesOnDisk,
            this.runFrequency,
            this.runFrequencyEnabled
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

        this.customPrefix.extend({
            required: {
                onlyIf: () => this.customPrefixEnabled()
            }
        });
        
        this.runFrequency.extend({
            required: {
                onlyIf: () => this.runFrequencyEnabled()
            }
        });


        this.validationGroup = ko.validatedObservable({
            connectionStringName: this.connectionStringName,
            olapTables: this.olapTables,
            transformationScripts: this.transformationScripts,
            mentorNode: this.mentorNode,
            customPrefix: this.customPrefix,
            runFrequency: this.runFrequency
        });
    }

    update(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskOlapEtlDetails) {
        super.update(dto);
        
        const configuration = dto.Configuration;
        if (configuration) {
            this.connectionStringName(configuration.ConnectionStringName);
            this.manualChooseMentor(!!configuration.MentorNode);
            
            this.customPrefix(configuration.CustomPrefix);
            this.customPrefixEnabled(!!configuration.CustomPrefix);
            
            this.keepFilesOnDisk(configuration.KeepFilesOnDisk);
                       
            this.runFrequencyEnabled(!!configuration.RunFrequency); 
            this.runFrequency(configuration.RunFrequency ||  ongoingTaskOlapEtlEditModel.defaultRunFrequency);
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
            CustomPrefix: this.customPrefixEnabled() ? this.customPrefix() : null,
            KeepFilesOnDisk: this.keepFilesOnDisk(),
            RunFrequency: this.runFrequencyEnabled() ? this.runFrequency() : null,
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
                    CustomPrefix: "",
                    KeepFilesOnDisk: false,
                    RunFrequency: this.defaultRunFrequency,
                    Transforms: [],
                    OlapTables: []
                }
            } as Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskOlapEtlDetails);
       }
}

export = ongoingTaskOlapEtlEditModel;

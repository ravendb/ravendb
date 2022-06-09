/// <reference path="../../../../typings/tsd.d.ts"/>
import ongoingTaskEditModel = require("models/database/tasks/ongoingTaskEditModel");
import ongoingTaskQueueEtlTransformationModel = require("models/database/tasks/ongoingTaskQueueEtlTransformationModel");
import jsonUtil = require("common/jsonUtil");
import QueueBroker = Raven.Client.Documents.Operations.ETL.Queue.QueueBroker;

class queueOptionsModel {
    queueName = ko.observable<string>();
    deleteProcessedDocuments = ko.observable<boolean>();

    validationGroup: KnockoutObservable<any>;
    dirtyFlag: () => DirtyFlag;

    constructor(name: string, deleteSource: boolean) {
        this.queueName(name);
        this.deleteProcessedDocuments(deleteSource);

        this.initValidation();

        this.dirtyFlag = new ko.DirtyFlag([
            this.queueName,
            this.deleteProcessedDocuments,
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }

    private initValidation() {
        this.queueName.extend({
            required: true
        });

        this.validationGroup = ko.validatedObservable({
            queueName: this.queueName
        });
    }

    static empty() {
        return new queueOptionsModel("", false);
    }
}

abstract class ongoingTaskQueueEtlEditModel extends ongoingTaskEditModel {
    
    connectionStringName = ko.observable<string>();
        
    allowEtlOnNonEncryptedChannel = ko.observable<boolean>(false);
    transformationScripts = ko.observableArray<ongoingTaskQueueEtlTransformationModel>([]);

    showEditTransformationArea: KnockoutComputed<boolean>;

    transformationScriptSelectedForEdit = ko.observable<ongoingTaskQueueEtlTransformationModel>();
    editedTransformationScriptSandbox = ko.observable<ongoingTaskQueueEtlTransformationModel>();
    
    optionsPerQueue = ko.observableArray<queueOptionsModel>();
    
    validationGroup: KnockoutValidationGroup;
    dirtyFlag: () => DirtyFlag;

    get studioTaskType(): StudioTaskType {
        return "KafkaQueueEtl";
    }
    
    constructor(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskQueueEtlDetails) {
        super();

        this.update(dto);
        this.initializeObservables();
        this.initValidation();
    }

    initializeObservables() {
        super.initializeObservables();
        
        this.showEditTransformationArea = ko.pureComputed(() => !!this.editedTransformationScriptSandbox());
        
        const innerDirtyFlag = ko.pureComputed(() => !!this.editedTransformationScriptSandbox() && this.editedTransformationScriptSandbox().dirtyFlag().isDirty());
        const scriptsCount = ko.pureComputed(() => this.transformationScripts().length);
        const hasAnyDirtyTransformationScript = ko.pureComputed(() => {
            let anyDirty = false;
            this.transformationScripts().forEach(script => {
                if (script.dirtyFlag().isDirty()) {
                    anyDirty = true;
                    // don't break here - we want to track all dependencies
                }
            });
            return anyDirty;
        });

        this.dirtyFlag = new ko.DirtyFlag([
                innerDirtyFlag,
                this.taskName,
                this.taskState,
                this.mentorNode,
                this.manualChooseMentor,
                this.connectionStringName,
                this.allowEtlOnNonEncryptedChannel,
                this.optionsPerQueue,
                scriptsCount,
                hasAnyDirtyTransformationScript
            ],
            false, jsonUtil.newLineNormalizingHashFunction);
    }
    
    private initValidation() {
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

        this.optionsPerQueue.extend({
            validation: [
                {
                    validator: () => {
                        const names = this.optionsPerQueue().map(x => x.queueName()).filter(x => !!x);
                        return new Set(names).size === this.optionsPerQueue().filter(x => !!x.queueName()).length;
                    },
                    message: "Duplicate queue names"
                }
            ]
        }); 

        this.validationGroup = ko.validatedObservable({
            connectionStringName: this.connectionStringName,
            mentorNode: this.mentorNode,
            transformationScripts: this.transformationScripts,
            optionsPerQueue: this.optionsPerQueue
        });
    }

    update(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskQueueEtlDetails) {
        super.update(dto);

        if (dto.Configuration) {
            this.connectionStringName(dto.Configuration.ConnectionStringName);
            this.transformationScripts(dto.Configuration.Transforms.map(x => new ongoingTaskQueueEtlTransformationModel(x, false, false)));
            this.manualChooseMentor(!!dto.Configuration.MentorNode);
            
            if (dto.Configuration.Queues) {
                dto.Configuration.Queues.forEach(x => {
                    const queueOptions = new queueOptionsModel(x.Name, x.DeleteProcessedDocuments);
                    this.optionsPerQueue.push(queueOptions);
                });
            }
        }
    }

    toDto(broker: QueueBroker): Raven.Client.Documents.Operations.ETL.Queue.QueueEtlConfiguration { 
        return {
            Name: this.taskName(),
            ConnectionStringName: this.connectionStringName(),
            AllowEtlOnNonEncryptedChannel: this.allowEtlOnNonEncryptedChannel(),
            Disabled: this.taskState() === "Disabled",
            Transforms: this.transformationScripts().map(x => x.toDto()),
            EtlType: "Queue",
            MentorNode: this.manualChooseMentor() ? this.mentorNode() : undefined,
            TaskId: this.taskId,
            BrokerType: broker,
            Queues: this.queueOptionsToDto()
        };
    }

    deleteTransformationScript(transformationScript: ongoingTaskQueueEtlTransformationModel) { 
        this.transformationScripts.remove(x => transformationScript.name() === x.name());
        
        if (this.transformationScriptSelectedForEdit() === transformationScript) {
            this.editedTransformationScriptSandbox(null);
            this.transformationScriptSelectedForEdit(null);
        }
    }

    editTransformationScript(transformationScript: ongoingTaskQueueEtlTransformationModel) {
        this.transformationScriptSelectedForEdit(transformationScript);
        this.editedTransformationScriptSandbox(new ongoingTaskQueueEtlTransformationModel(transformationScript.toDto(), false, transformationScript.resetScript()));
    }
    
    addNewQueueOptions() {
        this.optionsPerQueue.push(queueOptionsModel.empty());
    }

    removeQueueOptions(item: queueOptionsModel) {
        this.optionsPerQueue.remove(item);
    }

    queueOptionsToDto(): Array<Raven.Client.Documents.Operations.ETL.Queue.EtlQueue> {
        const result = [] as Array<Raven.Client.Documents.Operations.ETL.Queue.EtlQueue>;

        this.optionsPerQueue().forEach(x => {
            result.push({ Name: x.queueName(), DeleteProcessedDocuments: x.deleteProcessedDocuments() });
        });

        return result;
    }
    
    hasOptions() {
        return !!this.optionsPerQueue().length;
    }
}

export = ongoingTaskQueueEtlEditModel;

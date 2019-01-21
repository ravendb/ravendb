/// <reference path="../../../../typings/tsd.d.ts"/>
import generalUtils = require("common/generalUtils");

class pullReplicationDefinition {

    taskId: number;
    taskName = ko.observable<string>();
    taskType = ko.observable<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskType>();
    responsibleNode = ko.observable<Raven.Client.ServerWide.Operations.NodeId>();
    
    manualChooseMentor = ko.observable<boolean>(false);
    preferredMentor = ko.observable<string>();
    nodeTag: string = null;
    
    delayReplicationTime = ko.observable<number>();
    showDelayReplication = ko.observable<boolean>(false);
    humaneDelayDescription: KnockoutComputed<string>;
     
    validationGroup: KnockoutValidationGroup;

    constructor(dto: Raven.Client.Documents.Operations.Replication.PullReplicationDefinition) {
        this.update(dto); 
        this.initializeObservables();
        this.initValidation();
    }

    initializeObservables() {
        this.humaneDelayDescription = ko.pureComputed(() => {
            const delayTimeHumane = generalUtils.formatTimeSpan(this.delayReplicationTime() * 1000, true);
            return this.showDelayReplication() && this.delayReplicationTime.isValid() && this.delayReplicationTime() !== 0 ?
                `Documents will be replicated after a delay time of <strong>${delayTimeHumane}</strong>` : "";
        });
    }
    
    update(dto: Raven.Client.Documents.Operations.Replication.PullReplicationDefinition) {
        this.taskName(dto.Name);
        this.taskId = dto.TaskId;

        this.manualChooseMentor(!!dto.MentorNode);
        this.preferredMentor(dto.MentorNode);

        const delayTime = generalUtils.timeSpanToSeconds(dto.DelayReplicationFor);
        this.showDelayReplication(dto.DelayReplicationFor != null && delayTime !== 0);
        this.delayReplicationTime(dto.DelayReplicationFor ? delayTime : null);
        
        //TODO: certificates! 
    }

    toDto(taskId: number): Raven.Client.Documents.Operations.Replication.PullReplicationDefinition { 
        return {
            Name: this.taskName(),
            MentorNode: this.manualChooseMentor() ? this.preferredMentor() : undefined,
            TaskId: taskId,
            DelayReplicationFor: this.showDelayReplication() ? generalUtils.formatAsTimeSpan(this.delayReplicationTime() * 1000) : null,
            Certificates: null // TODO: !!!
        } as Raven.Client.Documents.Operations.Replication.PullReplicationDefinition;
    }

    initValidation() {
        this.taskName.extend({
            required: true
        });
        
        this.preferredMentor.extend({
            required: {
                onlyIf: () => this.manualChooseMentor()
            }
        });
        
        this.delayReplicationTime.extend({
            required: {
                onlyIf: () => this.showDelayReplication()
            },
            min: 0
        });
        
        //TODO: validation for certificates
        
        this.validationGroup = ko.validatedObservable({
            taskName: this.taskName,
            preferredMentor: this.preferredMentor,
            delayReplicationTime: this.delayReplicationTime            
        });
    }

    static empty(): pullReplicationDefinition {
        return new pullReplicationDefinition({  
            Name: "",
            DelayReplicationFor: null,
            Certificates: null,
            Disabled: false,
            MentorNode: null,
            TaskId: null
        } as Raven.Client.Documents.Operations.Replication.PullReplicationDefinition);
    }
}

export = pullReplicationDefinition;

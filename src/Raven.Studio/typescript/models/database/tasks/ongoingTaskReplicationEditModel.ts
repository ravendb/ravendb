/// <reference path="../../../../typings/tsd.d.ts"/>
import generalUtils = require("common/generalUtils");
import ongoingTaskEditModel = require("models/database/tasks/ongoingTaskEditModel"); 

class ongoingTaskReplicationEditModel extends ongoingTaskEditModel {
       
    connectionStringName = ko.observable<string>();
    
    delayReplicationTime = ko.observable<number>();
    showDelayReplication = ko.observable<boolean>(false);   
    humaneDelayDescription: KnockoutComputed<string>;   
     
    validationGroup: KnockoutValidationGroup;

    constructor(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskReplication) {
        super();

        this.update(dto); 
        this.initializeObservables();
        this.initValidation();
    }

    initializeObservables() {
     super.initializeObservables();

     this.humaneDelayDescription = ko.pureComputed(() => {
         const delayTimeHumane = generalUtils.formatTimeSpan(this.delayReplicationTime() * 1000, true);
         return this.showDelayReplication() && this.delayReplicationTime.isValid() && this.delayReplicationTime() !== 0 ? 
             `Documents will be replicated after a delay time of <strong>${delayTimeHumane}</strong>` : "";
     });
    }
    
    update(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskReplication) {
        super.update(dto);

        this.connectionStringName(dto.ConnectionStringName); 
        this.manualChooseMentor(!!dto.MentorNode);
        this.preferredMentor(dto.MentorNode);

        const delayTime = generalUtils.timeSpanToSeconds(dto.DelayReplicationFor);
        this.showDelayReplication(dto.DelayReplicationFor != null && delayTime !== 0);
        this.delayReplicationTime(dto.DelayReplicationFor ? delayTime : null);       
    }

    toDto(taskId: number): Raven.Client.Documents.Operations.Replication.ExternalReplication {
        return {
            Name: this.taskName(),
            MentorNode: this.manualChooseMentor() ? this.preferredMentor() : undefined,
            ConnectionStringName: this.connectionStringName(),
            TaskId: taskId,
            DelayReplicationFor: this.showDelayReplication() ? generalUtils.formatAsTimeSpan(this.delayReplicationTime() * 1000) : null,
        } as Raven.Client.Documents.Operations.Replication.ExternalReplication;
    }

    initValidation() {

        this.initializeMentorValidation();

        this.connectionStringName.extend({
            required: true
        });               

        this.delayReplicationTime.extend({
            required: {
                onlyIf: () => this.showDelayReplication()
            },
            min: 0
        });
        
        this.validationGroup = ko.validatedObservable({         
            connectionStringName: this.connectionStringName,
            preferredMentor: this.preferredMentor,
            delayReplicationTime: this.delayReplicationTime            
        });
    }

    static empty(): ongoingTaskReplicationEditModel {
        return new ongoingTaskReplicationEditModel({  
            TaskName: "",
            TaskType: "Replication",
            DestinationDatabase: null,
            DestinationUrl: null
        } as Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskReplication);
    }
}

export = ongoingTaskReplicationEditModel;

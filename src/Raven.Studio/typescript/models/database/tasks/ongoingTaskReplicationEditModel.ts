/// <reference path="../../../../typings/tsd.d.ts"/>
import generalUtils = require("common/generalUtils");
import ongoingTaskEditModel = require("models/database/tasks/ongoingTaskEditModel"); 

class ongoingTaskReplicationEditModel extends ongoingTaskEditModel {
       
    destinationDB = ko.observable<string>();          // Read-only data. Input data is through the connection string.   
    connectionStringName = ko.observable<string>();   // The connection string contains a list of discovery urls in the targeted cluster. The task communicates with these urls.
    
    delayReplicationTime = ko.observable<number>();
    showDelayReplication = ko.observable<boolean>(false);   
    humaneDelayDescription: KnockoutComputed<string>;   
     
    validationGroup: KnockoutValidationGroup;

    constructor(dto: Raven.Client.ServerWide.Operations.OngoingTaskReplication) {
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
    
    update(dto: Raven.Client.ServerWide.Operations.OngoingTaskReplication) {
        super.update(dto);

        this.connectionStringName(dto.ConnectionStringName); 
        this.destinationDB(dto.DestinationDatabase);
        this.manualChooseMentor(!!dto.MentorNode);
        this.preferredMentor(dto.MentorNode);

        const delayTime = generalUtils.timeSpanToSeconds(dto.DelayReplicationFor);
        this.showDelayReplication(dto.DelayReplicationFor != null && delayTime !== 0);
        this.delayReplicationTime(dto.DelayReplicationFor ? delayTime : null);       
    }

    toDto(taskId: number): Raven.Client.ServerWide.ExternalReplication {
        return {
            Name: this.taskName(),
            Database: this.destinationDB(),
            MentorNode: this.manualChooseMentor() ? this.preferredMentor() : undefined,
            ConnectionStringName: this.connectionStringName(),
            TaskId: taskId,
            DelayReplicationFor: this.showDelayReplication() ? generalUtils.formatAsTimeSpan(this.delayReplicationTime() * 1000) : null,
        } as Raven.Client.ServerWide.ExternalReplication;
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
        } as Raven.Client.ServerWide.Operations.OngoingTaskReplication);
    }
}

export = ongoingTaskReplicationEditModel;

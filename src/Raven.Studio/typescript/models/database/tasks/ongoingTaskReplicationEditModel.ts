/// <reference path="../../../../typings/tsd.d.ts"/>
import ongoingTaskEditModel = require("models/database/tasks/ongoingTaskEditModel"); 

class ongoingTaskReplicationEditModel extends ongoingTaskEditModel {
       
    destinationDB = ko.observable<string>();        // Read-only data. Input data is through the connection string.
    destinationURL = ko.observable<string>();       // Actual destination url where the targeted database is located. Read-only data.
    connectionStringName = ko.observable<string>(); // Contains list of discovery urls in the targeted cluster. The task communicates with these urls.
    
    showReplicationDetails = ko.observable(false);
  
    validationGroup: KnockoutValidationGroup;

    constructor(dto: Raven.Client.ServerWide.Operations.OngoingTaskReplication) {
        super();

        this.update(dto); 
        this.initializeObservables();
        this.initValidation();
    }
    
    update(dto: Raven.Client.ServerWide.Operations.OngoingTaskReplication) {
        super.update(dto);

        this.connectionStringName(dto.ConnectionStringName); 
        this.destinationDB(dto.DestinationDatabase);
        this.destinationURL(dto.DestinationUrl || 'N/A');
        this.manualChooseMentor(!!dto.MentorNode);
        this.preferredMentor(dto.MentorNode);
    }

    toDto(taskId: number): Raven.Client.ServerWide.ExternalReplication {
        return {
            Name: this.taskName(),
            Database: this.destinationDB(),
            MentorNode: this.manualChooseMentor() ? this.preferredMentor() : undefined,
            ConnectionStringName: this.connectionStringName(),
            TaskId: taskId
        } as Raven.Client.ServerWide.ExternalReplication;
    }

    initValidation() {

        this.initializeMentorValidation();

        this.connectionStringName.extend({
            required: true
        });               

        this.validationGroup = ko.validatedObservable({         
            connectionStringName: this.connectionStringName,
            preferredMentor: this.preferredMentor
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

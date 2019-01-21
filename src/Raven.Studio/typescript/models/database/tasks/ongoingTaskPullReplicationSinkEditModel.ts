/// <reference path="../../../../typings/tsd.d.ts"/>
import ongoingTaskEditModel = require("models/database/tasks/ongoingTaskEditModel"); 

class ongoingTaskPullReplicationSinkEditModel extends ongoingTaskEditModel {

    hubDefinitionName = ko.observable<string>();
    connectionStringName = ko.observable<string>();

    validationGroup: KnockoutValidationGroup;

    constructor(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskPullReplicationAsSink) {
        super();

        this.update(dto);
        this.initializeObservables();
        this.initValidation();
    }

    update(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskPullReplicationAsSink) {
        super.update(dto);

        this.connectionStringName(dto.ConnectionStringName);
        this.manualChooseMentor(!!dto.MentorNode);
        this.preferredMentor(dto.MentorNode);
        this.hubDefinitionName(dto.HubDefinitionName);
    }

    toDto(taskId: number): Raven.Client.Documents.Operations.Replication.PullReplicationAsSink {
        return {
            Name: this.taskName(),
            MentorNode: this.manualChooseMentor() ? this.preferredMentor() : undefined,
            ConnectionStringName: this.connectionStringName(),
            TaskId: taskId,
            HubDefinitionName: this.hubDefinitionName(),
            CertificatePassword: null, //TODO,
            CertificateWithPrivateKey: null, //TODO,
        } as Raven.Client.Documents.Operations.Replication.PullReplicationAsSink;
    }

    initValidation() {

        this.initializeMentorValidation();

        this.connectionStringName.extend({
            required: true
        });

        this.hubDefinitionName.extend({
            required: true
        });

        this.validationGroup = ko.validatedObservable({
            connectionStringName: this.connectionStringName,
            preferredMentor: this.preferredMentor,
            hubDefinitionName: this.hubDefinitionName
        });
    }

    static empty(): ongoingTaskPullReplicationSinkEditModel {
        return new ongoingTaskPullReplicationSinkEditModel({
            TaskType: "PullReplicationAsSink",
            TaskName: ""
        } as Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskPullReplicationAsSink);
    }
    
}

export = ongoingTaskPullReplicationSinkEditModel;

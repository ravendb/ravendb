/// <reference path="../../../../typings/tsd.d.ts"/>
import ongoingTaskEditModel = require("models/database/tasks/ongoingTaskEditModel");
import pullReplicationCertificate = require("models/database/tasks/pullReplicationCertificate"); 

class ongoingTaskPullReplicationSinkEditModel extends ongoingTaskEditModel {

    hubDefinitionName = ko.observable<string>();
    connectionStringName = ko.observable<string>();

    certificate = ko.observable<pullReplicationCertificate>();

    certificateAsBase64 = ko.observable<string>();
    certificatePassphrase = ko.observable<string>();

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
        this.hubDefinitionName(dto.HubDefinitionName);
        this.certificate(dto.CertificatePublicKey ? new pullReplicationCertificate(dto.CertificatePublicKey) : null);
    }

    toDto(taskId: number): Raven.Client.Documents.Operations.Replication.PullReplicationAsSink {
        const certificate = this.certificate() ? this.certificate().certificate() : undefined;
        const certificatePassphrase = this.certificate() ? this.certificate().certificatePassphrase() : undefined;
        return {
            Name: this.taskName(),
            MentorNode: this.manualChooseMentor() ? this.mentorNode() : undefined,
            ConnectionStringName: this.connectionStringName(),
            TaskId: taskId,
            HubDefinitionName: this.hubDefinitionName(),
            CertificatePassword: certificatePassphrase,
            CertificateWithPrivateKey: certificate
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
            mentorNode: this.mentorNode,
            hubDefinitionName: this.hubDefinitionName,
            certificate: this.certificate
        });
    }
    
    static empty(): ongoingTaskPullReplicationSinkEditModel {
        return new ongoingTaskPullReplicationSinkEditModel({
            TaskType: "PullReplicationAsSink",
            TaskName: ""
        } as Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskPullReplicationAsSink);
    }

    tryReadCertificate(): boolean {
        const cert = this.certificateAsBase64();
        const password = this.certificatePassphrase() || undefined;

        try {
            this.certificate(pullReplicationCertificate.fromPkcs12(cert, password));

            this.certificateAsBase64(null);
            this.certificatePassphrase("");
            return true;
        } catch (e) {
            return false;
        }
    }
    
}

export = ongoingTaskPullReplicationSinkEditModel;

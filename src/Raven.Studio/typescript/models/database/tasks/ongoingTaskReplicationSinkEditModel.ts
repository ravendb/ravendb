/// <reference path="../../../../typings/tsd.d.ts"/>
import ongoingTaskEditModel = require("models/database/tasks/ongoingTaskEditModel");
import replicationCertificateModel = require("models/database/tasks/replicationCertificateModel");
import replicationAccessSinkModel = require("models/database/tasks/replicationAccessSinkModel");
import prefixPathModel = require("models/database/tasks/prefixPathModel");

class ongoingTaskReplicationSinkEditModel extends ongoingTaskEditModel {

    hubName = ko.observable<string>();
    connectionStringName = ko.observable<string>();
    
    allowReplicationFromHubToSink = ko.observable<boolean>(true);
    allowReplicationFromSinkToHub = ko.observable<boolean>();
    replicationMode: KnockoutComputed<Raven.Client.Documents.Operations.Replication.PullReplicationMode>;
    
    replicationAccess = ko.observable<replicationAccessSinkModel>();
    
    validationGroup: KnockoutValidationGroup;

    constructor(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskPullReplicationAsSink, 
                private serverCertificate: replicationCertificateModel) {
        super();

        this.update(dto);
        
        this.initializeObservables();
        this.initValidation();
    }
    
    initializeObservables() {
        super.initializeObservables();
        
        this.replicationMode = ko.pureComputed(() => {

            if (this.allowReplicationFromHubToSink() && this.allowReplicationFromSinkToHub()) {
                return "HubToSink,SinkToHub" as Raven.Client.Documents.Operations.Replication.PullReplicationMode;
            }

            return (this.allowReplicationFromHubToSink()) ? "HubToSink" :
                this.allowReplicationFromSinkToHub() ? "SinkToHub" : "None";
        })
    }

    update(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskPullReplicationAsSink) {
        super.update(dto);

        this.connectionStringName(dto.ConnectionStringName);
        this.manualChooseMentor(!!dto.MentorNode);
        this.hubName(dto.HubDefinitionName);

        this.allowReplicationFromHubToSink(dto.Mode.includes("HubToSink"));
        this.allowReplicationFromSinkToHub(dto.Mode.includes("SinkToHub"));
        
        const accessInfo = new replicationAccessSinkModel(
            dto.AccessName,
            dto.CertificatePublicKey ? new replicationCertificateModel(dto.CertificatePublicKey) : null,
            this.serverCertificate,
            dto.AllowedHubToSinkPaths ? dto.AllowedHubToSinkPaths.map(x => new prefixPathModel(x)) : [],
            dto.AllowedSinkToHubPaths ? dto.AllowedSinkToHubPaths.map(x => new prefixPathModel(x)) : []);
        
        this.replicationAccess(accessInfo);
    }

    toDto(taskId: number): Raven.Client.Documents.Operations.Replication.PullReplicationAsSink {
        const accessInfo = this.replicationAccess();
        
        const certificate = accessInfo.certificate() ? accessInfo.certificate().certificate() : undefined;
        const certificatePassphrase = accessInfo.certificate() ? accessInfo.certificate().certificatePassphrase() : undefined;
        
        return {
            TaskId: taskId,
            Name: this.taskName(),
            MentorNode: this.manualChooseMentor() ? this.mentorNode() : undefined,
            ConnectionStringName: this.connectionStringName(),
            HubName: this.hubName(),
            Mode: this.replicationMode(),
            AccessName: accessInfo.replicationAccessName(),
            CertificatePassword: certificatePassphrase,
            CertificateWithPrivateKey: this.replicationAccess().serverCertificateSelected() ? null : certificate,
            AllowedHubToSinkPaths: accessInfo.hubToSinkPrefixes()?.map(x => x.path()),
            AllowedSinkToHubPaths: accessInfo.sinkToHubPrefixes()?.map(x => x.path())
        } as Raven.Client.Documents.Operations.Replication.PullReplicationAsSink;
    }

    initValidation() {
        this.replicationMode.extend({
            validation: [
                {
                    validator: () => this.replicationMode() !== "None",
                    message: "Please select at least one replication mode"
                }
            ]
        })
        
        this.initializeMentorValidation();

        this.connectionStringName.extend({
            required: true
        });

        this.hubName.extend({
            required: true
        });
        
        this.validationGroup = ko.validatedObservable({
            hubDefinitionName: this.hubName,
            mentorNode: this.mentorNode,
            connectionStringName: this.connectionStringName
        });
    }
    
    static empty(serverCertificate: replicationCertificateModel): ongoingTaskReplicationSinkEditModel {
        return new ongoingTaskReplicationSinkEditModel({
            TaskType: "ReplicationAsSink" as Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskType,
            TaskName: "",
            AccessName: "",
            AllowedHubToSinkPaths: null,
            AllowedSinkToHubPaths: null,
            HubDefinitionName: "",
            Mode: "HubToSink"
        } as Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskPullReplicationAsSink, serverCertificate);
    }
}

export = ongoingTaskReplicationSinkEditModel;

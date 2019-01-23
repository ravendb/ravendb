/// <reference path="../../../../typings/tsd.d.ts"/>
import generalUtils = require("common/generalUtils");
import pullReplicationCertificate = require("models/database/tasks/pullReplicationCertificate");

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
    
    certificates = ko.observableArray<pullReplicationCertificate>([]);

    certificateGenerated: KnockoutComputed<boolean>;
    certificateExported = ko.observable<boolean>(false);
     
    validationGroup: KnockoutValidationGroup;
    exportValidationGroup: KnockoutValidationGroup;

    constructor(dto: Raven.Client.Documents.Operations.Replication.PullReplicationDefinition, requiresCertificates: boolean) {
        this.update(dto); 
        this.initializeObservables();
        this.initValidation(requiresCertificates);
    }

    initializeObservables() {
        this.humaneDelayDescription = ko.pureComputed(() => {
            const delayTimeHumane = generalUtils.formatTimeSpan(this.delayReplicationTime() * 1000, true);
            return this.showDelayReplication() && this.delayReplicationTime.isValid() && this.delayReplicationTime() !== 0 ?
                `Documents will be replicated after a delay time of <strong>${delayTimeHumane}</strong>` : "";
        });

        this.certificateGenerated = ko.pureComputed(() => {
            return _.some(this.certificates(), x => !!x.certificate());
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
        
        if (dto.Certificates) {
            this.certificates(_.map(dto.Certificates, value => new pullReplicationCertificate(value)));
        }
    }

    toDto(taskId: number): Raven.Client.Documents.Operations.Replication.PullReplicationDefinition { 
        const certificates = {} as dictionary<string>;
        this.certificates().forEach(cert => {
            certificates[cert.thumbprint()] = cert.publicKey();
        });
        
        return {
            Name: this.taskName(),
            MentorNode: this.manualChooseMentor() ? this.preferredMentor() : undefined,
            TaskId: taskId,
            DelayReplicationFor: this.showDelayReplication() ? generalUtils.formatAsTimeSpan(this.delayReplicationTime() * 1000) : null,
            Certificates: certificates
        } as Raven.Client.Documents.Operations.Replication.PullReplicationDefinition;
    }

    getCertificate() {
        const certificate = this.certificates().find(x => !!x.certificate());
        return certificate ? certificate.certificate() : null;
    }
    
    initValidation(requiresCertificates: boolean) {
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
        
        if (requiresCertificates) {
            this.certificates.extend({
                validation: [
                    {
                        validator: (cert: Array<pullReplicationCertificate>) => cert.length > 0,
                        message: "Please define at least one certificate"
                    }
                ]
            });

            this.certificateExported.extend({
                validation: [
                    {
                        validator: (v: boolean) => {
                            const required = this.certificateGenerated();
                            return !required || v;
                        },
                        message: "Please download or export generated certificate"
                    }
                ]
            });
        }
        
        this.validationGroup = ko.validatedObservable({
            taskName: this.taskName,
            preferredMentor: this.preferredMentor,
            delayReplicationTime: this.delayReplicationTime,
            certificates: this.certificates,
            certificateExported: this.certificateExported
        });

        this.exportValidationGroup = ko.validatedObservable({
            taskName: this.taskName,
            preferredMentor: this.preferredMentor,
            delayReplicationTime: this.delayReplicationTime,
            certificates: this.certificates
        });
    }

    static empty(requiresCertificates: boolean): pullReplicationDefinition {
        return new pullReplicationDefinition({  
            Name: "",
            DelayReplicationFor: null,
            Certificates: null,
            Disabled: false,
            MentorNode: null,
            TaskId: null
        } as Raven.Client.Documents.Operations.Replication.PullReplicationDefinition, requiresCertificates);
    }
}

export = pullReplicationDefinition;

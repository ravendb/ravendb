/// <reference path="../../../../typings/tsd.d.ts"/>

abstract class ongoingTask { 

    taskType = ko.observable<Raven.Server.Web.System.OngoingTaskType>();
    responsibleNode = ko.observable<Raven.Client.Server.Operations.NodeId>();
    taskState = ko.observable<Raven.Server.Web.System.OngoingTaskState>();
    lastModificationTime = ko.observable<string>();
    taskConnectionStatus = ko.observable<Raven.Server.Web.System.OngoingTaskConnectionStatus>();
    
    badgeText: KnockoutComputed<string>;
    badgeClass: KnockoutComputed<string>;

    constructor(dto: Raven.Server.Web.System.OngoingTask) {
    }

    protected initializeObservables() {
        
        this.badgeClass = ko.pureComputed(() => {
            if (this.taskState() === 'Enabled') {
                return "state-success";
            }

            if (this.taskState() === "Disabled") {
                return "state-danger";
            }

            if (this.taskState() === "PartiallyEnabled") {
                return "state-warning";
            }

            return "state-offline"; // ? 
        });

        this.badgeText = ko.pureComputed(() => {
            if (this.taskState() === 'Enabled') {
                return "Enabled";
            }

            if (this.taskState() === "Disabled") {
                return "Disabled";
            }
            if (this.taskState() === "PartiallyEnabled") {
                return "Partial";
            }
            
            return "Offline"; // ?
        });

    }

    protected update(dto: Raven.Server.Web.System.OngoingTask) {
        this.taskType(dto.TaskType);
        this.responsibleNode(dto.ResponsibleNode);
        this.taskState(dto.TaskState);
        this.lastModificationTime(dto.LastModificationTime);
        this.taskConnectionStatus(dto.TaskConnectionStatus);
    }
}

export = ongoingTask;

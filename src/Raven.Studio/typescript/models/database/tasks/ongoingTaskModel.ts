/// <reference path="../../../../typings/tsd.d.ts"/>

abstract class ongoingTaskModel { 

    taskId: number;
    taskName = ko.observable<string>();
    taskType = ko.observable<Raven.Client.ServerWide.Operations.OngoingTaskType>();
    responsibleNode = ko.observable<Raven.Client.ServerWide.Operations.NodeId>();
    taskState = ko.observable<Raven.Client.ServerWide.Operations.OngoingTaskState>();
    taskConnectionStatus = ko.observable<Raven.Client.ServerWide.Operations.OngoingTaskConnectionStatus>(); // TODO: discuss this property...
    
    badgeText: KnockoutComputed<string>;
    badgeClass: KnockoutComputed<string>;
   
    isInTasksListView: boolean = true;

    protected initializeObservables() {
        
        this.badgeClass = ko.pureComputed(() => {
            if (this.taskState() === 'Enabled') {
                return "state-success";
            }

            if (this.taskState() === "Disabled") {
                return "state-warning";
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

    protected update(dto: Raven.Client.ServerWide.Operations.OngoingTask) {
        if (this.isInTasksListView) {
            this.updateTaskNameIfNeeded(dto);
        }

        this.taskId = dto.TaskId;
        this.taskName(dto.TaskName);
        this.taskType(dto.TaskType);
        this.responsibleNode(dto.ResponsibleNode);
        this.taskState(dto.TaskState);
        this.taskConnectionStatus(dto.TaskConnectionStatus);
    }

    private updateTaskNameIfNeeded(dto: Raven.Client.ServerWide.Operations.OngoingTask) {
        dto.TaskName = dto.TaskName ? dto.TaskName.trim() : dto.TaskName; 
        if (dto.TaskName) { 
            return;
        }

        dto.TaskName = this.generateTaskName(dto);
    }

    protected abstract generateTaskName(dto: Raven.Client.ServerWide.Operations.OngoingTask): string;
}

export = ongoingTaskModel;

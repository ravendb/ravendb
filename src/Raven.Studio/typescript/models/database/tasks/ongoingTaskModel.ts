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
            dto.TaskName = ongoingTaskModel.generateTaskNameIfNeeded(dto);
        }

        this.taskId = dto.TaskId;
        this.taskName(dto.TaskName);
        this.taskType(dto.TaskType);
        this.responsibleNode(dto.ResponsibleNode);
        this.taskState(dto.TaskState);
        this.taskConnectionStatus(dto.TaskConnectionStatus);
    }

    static generateTaskNameIfNeeded(dto: Raven.Client.ServerWide.Operations.OngoingTask): string {
        dto.TaskName = dto.TaskName ? dto.TaskName.trim() : dto.TaskName;
        return dto.TaskName || ongoingTaskModel.generateTaskName(dto);
    }

    static generateTaskName(dto: Raven.Client.ServerWide.Operations.OngoingTask): string {
        // Note: This is static because it is also being called from other places (i.e. databaseGroupGraph.ts) which don't have the tasks models objects..
        let taskName: string = "";

        switch (dto.TaskType) { 
            case "Replication":
                const dtoReplication = dto as Raven.Client.ServerWide.Operations.OngoingTaskReplication;
                taskName = `External replication to ${dtoReplication.DestinationDatabase}@${dtoReplication.DestinationUrl}`;
                break;
            case "Backup":
                const dtoBackup = dto as Raven.Client.ServerWide.Operations.OngoingTaskBackup;
                taskName = dtoBackup.BackupDestinations.length === 0 ? "No destinations" : `${dtoBackup.BackupType} to ${dtoBackup.BackupDestinations.join(", ")}`;
                break;
            case "RavenEtl":
                const dtoRavenEtl = dto as Raven.Client.ServerWide.Operations.OngoingTaskRavenEtl;
                taskName = `ETL to ${dtoRavenEtl.DestinationDatabase}@${dtoRavenEtl.DestinationUrl}`;
                break;
            case "SqlEtl":
                const dtoSqlEtl = dto as Raven.Client.ServerWide.Operations.OngoingTaskSqlEtl;
                taskName = ""; // Todo...
                break;
            case "Subscription":
                taskName = dto.TaskName;
                break;
        }

        return taskName;
    } 
}

export = ongoingTaskModel;

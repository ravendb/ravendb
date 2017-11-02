/// <reference path="../../../../typings/tsd.d.ts"/>

abstract class ongoingTaskModel { 

    taskId: number;
    taskName = ko.observable<string>();
    taskType = ko.observable<Raven.Client.ServerWide.Operations.OngoingTaskType>();
    responsibleNode = ko.observable<Raven.Client.ServerWide.Operations.NodeId>();
    taskState = ko.observable<Raven.Client.ServerWide.Operations.OngoingTaskState>();
    taskConnectionStatus = ko.observable<Raven.Client.ServerWide.Operations.OngoingTaskConnectionStatus>();
    
    badgeText: KnockoutComputed<string>;
    badgeClass: KnockoutComputed<string>;
    stateText: KnockoutComputed<string>;

    protected initializeObservables() {
        
        this.badgeClass = ko.pureComputed(() => {
            if (this.taskConnectionStatus() === 'Active') {
                return "state-success";
            }
            if (this.taskConnectionStatus() === "Reconnect") {
                return "state-warning";
            }
            if (this.taskConnectionStatus() === "NotActive") {
                return "state-warning";
            }
            if (this.taskConnectionStatus() === "NotOnThisNode") {
                return "state-offline";
            }
            if (this.taskConnectionStatus() === "None") {
                return "state-offline";
            }
        });

        this.badgeText = ko.pureComputed(() => {
            if (this.taskConnectionStatus() === "None") {
                return "None";
            }
            if (this.taskConnectionStatus() === 'Active') {
                return "Active";
            }
            if (this.taskConnectionStatus() === "NotActive") {
                return "Not Active";
            }
            if (this.taskConnectionStatus() === "NotOnThisNode") {
                return "Not On Node";
            }
            if (this.taskConnectionStatus() === "Reconnect") {
                return "Reconnect";
            }
        });

        this.stateText = ko.pureComputed(() => {
            if (this.taskState() === 'Enabled') {
                return "Enabled";
            }
            if (this.taskState() === "Disabled") {
                return "Disabled";
            }
            if (this.taskState() === "PartiallyEnabled") {
                return "Partial";
                // Relevant only for Etl tasks with some disabled scripts...
                // Todo: to be handled in issue 8880
            }
        });
    }

    protected update(dto: Raven.Client.ServerWide.Operations.OngoingTask) {
        this.taskName(ongoingTaskModel.generateTaskNameIfNeeded(dto));
        this.taskId = dto.TaskId;
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
                taskName = `External replication to ${dtoReplication.DestinationDatabase}@${dtoReplication.DestinationUrl || 'N/A'}`;
                break;
            case "Backup":
                const dtoBackup = dto as Raven.Client.ServerWide.Operations.OngoingTaskBackup;
                taskName = dtoBackup.BackupDestinations.length === 0 ? "Backup w/o destinations" : `${dtoBackup.BackupType} to ${dtoBackup.BackupDestinations.join(", ")}`;
                break;
            case "RavenEtl":
                const dtoRavenEtl = dto as Raven.Client.ServerWide.Operations.OngoingTaskRavenEtlListView;
                taskName = `ETL to ${dtoRavenEtl.DestinationDatabase}@${dtoRavenEtl.DestinationUrl || 'N/A'}`;
                break;
            case "SqlEtl":
                const dtoSqlEtl = dto as Raven.Client.ServerWide.Operations.OngoingTaskSqlEtlListView;
                taskName = `SQL ETL to ${dtoSqlEtl.DestinationDatabase}@${dtoSqlEtl.DestinationServer}`;
                break;
            case "Subscription":
                taskName = dto.TaskName;
                break;
        }

        return taskName;
    } 
}

export = ongoingTaskModel;

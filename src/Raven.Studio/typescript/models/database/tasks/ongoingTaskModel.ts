/// <reference path="../../../../typings/tsd.d.ts"/>

abstract class ongoingTaskModel { 

    taskId: number;
    taskName = ko.observable<string>();
    taskType = ko.observable<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskType>();
    responsibleNode = ko.observable<Raven.Client.ServerWide.Operations.NodeId>();
    taskState = ko.observable<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskState>();
    taskConnectionStatus = ko.observable<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskConnectionStatus>();
    
    badgeText: KnockoutComputed<string>;
    badgeClass: KnockoutComputed<string>;
    stateText: KnockoutComputed<string>;

    protected initializeObservables() {
        
        this.badgeClass = ko.pureComputed(() => {
            switch (this.taskConnectionStatus()) {
                case 'Active': {
                    return "state-success";
                }
                case 'Reconnect': {
                    return "state-warning";
                }
                case 'NotActive': {
                    return "state-warning";
                }
                case 'NotOnThisNode': {
                    return "state-offline";
                }
                case 'None': {
                    return "state-offline";
                }
            }
        });            

        this.badgeText = ko.pureComputed(() => {

            switch (this.taskConnectionStatus()) {
                case 'Active': {
                    return "Active";
                }
                case 'Reconnect': {
                    return "Reconnect";
                }
                case 'NotActive': {
                    return "Not Active";
                }
                case 'NotOnThisNode': {
                    return "Not On Node";
                }
                case 'None': {
                    return "None";
                }
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

    protected update(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTask) {
        this.taskName(ongoingTaskModel.generateTaskNameIfNeeded(dto));
        this.taskId = dto.TaskId;
        this.taskType(dto.TaskType);
        this.responsibleNode(dto.ResponsibleNode);
        this.taskState(dto.TaskState);
        this.taskConnectionStatus(dto.TaskConnectionStatus);
    }

    static generateTaskNameIfNeeded(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTask): string {
        dto.TaskName = dto.TaskName ? dto.TaskName.trim() : dto.TaskName;
        return dto.TaskName || ongoingTaskModel.generateTaskName(dto);
    }

    static generateTaskName(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTask): string {
        // Note: This is static because it is also being called from other places (i.e. databaseGroupGraph.ts) which don't have the tasks models objects..
        let taskName: string = "";

        switch (dto.TaskType) { 
            case "Replication":
                const dtoReplication = dto as Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskReplication;
                taskName = `External replication to ${dtoReplication.DestinationDatabase}@${dtoReplication.DestinationUrl || 'N/A'}`;
                break;
            case "Backup":
                const dtoBackup = dto as Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskBackup;
                taskName = dtoBackup.BackupDestinations.length === 0 ? "Backup w/o destinations" : `${dtoBackup.BackupType} to ${dtoBackup.BackupDestinations.join(", ")}`;
                break;
            case "RavenEtl":
                const dtoRavenEtl = dto as Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskRavenEtlListView;
                taskName = `ETL to ${dtoRavenEtl.DestinationDatabase}@${dtoRavenEtl.DestinationUrl || 'N/A'}`;
                break;
            case "SqlEtl":
                const dtoSqlEtl = dto as Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskSqlEtlListView;
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

import OngoingTaskState = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskState;
import ServerWideTaskDto = Raven.Server.Web.System.AdminStudioServerWideHandler.ServerWideTasksResult.ServerWideTask;
import ServerWideBackupTaskDto = Raven.Server.Web.System.AdminStudioServerWideHandler.ServerWideTasksResult.ServerWideBackupTask;
import ServerWideExternalReplicationTaskDto = Raven.Server.Web.System.AdminStudioServerWideHandler.ServerWideTasksResult.ServerWideExternalReplicationTask;

export type ServerWideTaskType = "Backup" | "Replication";

export interface ServerWideTaskSharedInfo {
    taskId: number;
    taskName: string;
    taskType: ServerWideTaskType;
    taskState: OngoingTaskState;
    responsibleNodeTag: string;
    excludedDatabases: string[];
}

export interface ServerWideBackupTaskInfo extends ServerWideTaskSharedInfo {
    taskType: "Backup";
    backupType: Raven.Client.Documents.Operations.Backups.BackupType;
    backupDestinations: string[];
    isEncrypted: boolean;
    retentionPolicy: Raven.Client.Documents.Operations.Backups.RetentionPolicy;
}

export interface ServerWideExternalReplicationTaskInfo extends ServerWideTaskSharedInfo {
    taskType: "Replication";
    delayReplicationFor: string;
    topologyDiscoveryUrls: string[];
}

export type ServerWideTaskInfo = ServerWideBackupTaskInfo | ServerWideExternalReplicationTaskInfo;

function mapSharedInfo(dto: ServerWideTaskDto): Omit<ServerWideTaskSharedInfo, "taskType"> {
    return {
        taskId: dto.TaskId,
        taskName: dto.TaskName,
        taskState: dto.TaskState,
        responsibleNodeTag: dto.ResponsibleNode?.NodeTag ?? null,
        excludedDatabases: dto.ExcludedDatabases ?? [],
    };
}

export function mapServerWideTaskFromDto(dto: ServerWideTaskDto): ServerWideTaskInfo {
    switch (dto.TaskType) {
        case "Backup": {
            const backupDto = dto as ServerWideBackupTaskDto;
            return {
                ...mapSharedInfo(dto),
                taskType: "Backup",
                backupType: backupDto.BackupType,
                backupDestinations: backupDto.BackupDestinations ?? [],
                isEncrypted: backupDto.IsEncrypted,
                retentionPolicy: backupDto.RetentionPolicy,
            };
        }
        case "Replication": {
            const replicationDto = dto as ServerWideExternalReplicationTaskDto;
            return {
                ...mapSharedInfo(dto),
                taskType: "Replication",
                delayReplicationFor: replicationDto.DelayReplicationFor,
                topologyDiscoveryUrls: replicationDto.TopologyDiscoveryUrls ?? [],
            };
        }
        default:
            throw new Error("Unexpected server-wide task type: " + dto.TaskType);
    }
}

import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

type PeriodicBackupStatus = Raven.Client.Documents.Operations.Backups.PeriodicBackupStatus;

interface PeriodicBackupStatusResponse {
    IsSharded: boolean;
    Status?: PeriodicBackupStatus;
    Statuses?: Record<string, PeriodicBackupStatus>;
}

class getPeriodicBackupStatusCommand extends commandBase {
    private readonly databaseName: string;
    private readonly taskId: number;

    constructor(databaseName: string, taskId: number) {
        super();
        this.databaseName = databaseName;
        this.taskId = taskId;
    }

    execute(): JQueryPromise<PeriodicBackupStatus[]> {
        const args = {
            name: this.databaseName,
            taskId: this.taskId,
        };

        const url = endpoints.global.backupDatabase.periodicBackupStatus;

        return this.query(url, args, null, (response: PeriodicBackupStatusResponse) =>
            (response.IsSharded ? Object.values(response.Statuses ?? {}) : [response.Status]).filter((x) => x != null)
        );
    }
}

export = getPeriodicBackupStatusCommand;

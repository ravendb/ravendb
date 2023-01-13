import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");


class delayBackupCommand extends commandBase {
    private db: database;

    private readonly taskId: number;

    private readonly duration: string;

    constructor(db: database, taskId: number, duration: string) {
        super();
        this.duration = duration;
        this.taskId = taskId;
        this.db = db;
    }
 
    execute(): JQueryPromise<Raven.Client.Documents.Operations.Backups.StartBackupOperationResult> {
        const args = {
            taskId: this.taskId,
            duration: this.duration,
            database: this.db.name
        }
        const url = endpoints.global.adminDatabases.adminBackupTaskDelay + this.urlEncodeArgs(args);
        
        return this.post(url, null, null,  { dataType: undefined })
            .done(() => this.reportSuccess("Backup was delayed"))
            .fail(response => this.reportError("Failed to delay backup task", response.responseText, response.statusText));
    }
}

export = delayBackupCommand;

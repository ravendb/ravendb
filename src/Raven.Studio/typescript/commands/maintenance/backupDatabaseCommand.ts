import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import getDocumentWithMetadataCommand = require("commands/database/documents/getDocumentWithMetadataCommand");

class backupDatabaseCommand extends commandBase {

    constructor(private db: database, private backupLocation: string, private updateBackupStatus: (status: backupStatusDto) => void, private incremental: boolean) {
        super();
    }

    execute(): JQueryPromise<any> {
        var result = $.Deferred();

        this.query('/admin/databases/' + this.db.name, null, null /* We should query the system URL here */)//TODO: use endpoints
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to fetch database document!", response.responseText, response.statusText);
                result.reject();
            })
            .done((doc: databaseDocumentDto) => {
                var args: backupRequestDto = {
                    BackupLocation: this.backupLocation,
                    DatabaseDocument: doc
                };
                this.post('/admin/backup?incremental=' + this.incremental, JSON.stringify(args), this.db, { dataType: 'text' })//TODO: use endpoints
                    .fail((response: JQueryXHR) => {
                        this.reportError("Failed to create backup!", response.responseText, response.statusText);
                        result.reject();
                    })
                    .done(() => this.getBackupStatus(result));
                });

        return result;
    }

    private getBackupStatus(result: JQueryDeferred<any>) {
        new getDocumentWithMetadataCommand("Raven/Backup/Status", this.db)
            .execute()
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to fetch backup status!", response.responseText, response.statusText);
                result.reject();
            })
            .done((backupStatus: backupStatusDto)=> {
                this.updateBackupStatus(backupStatus);
                if (backupStatus.IsRunning) {
                    setTimeout(() => this.getBackupStatus(result), 1000);
                } else {
                    if (backupStatus.Success) {
                        this.reportSuccess("Database backup was successfully created!");
                    } else {
                        var cause = backupStatus.Messages.last(x => x.Severity === 'Error');
                        this.reportError("Failed to backup database: " + cause.Message);
                    }
                    
                    result.resolve();
                }
            });
    }
}

export = backupDatabaseCommand;

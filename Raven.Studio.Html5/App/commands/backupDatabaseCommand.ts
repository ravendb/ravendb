import commandBase = require("commands/commandBase");
import database = require("models/database");
import getDocumentWithMetadataCommand = require("commands/getDocumentWithMetadataCommand");

class backupDatabaseCommand extends commandBase {

  constructor(private db: database, private backupLocation: string, private updateBackupStatus: (backupStatusDto) => void) {
    super();
  }

  execute(): JQueryPromise<any> {
    var result = $.Deferred();

    this.query('/admin/databases/' + this.db.name, null, null /* We should query the system URL here */)
      .fail(response=> result.reject(response))
      .done((doc: databaseDocumentDto)=> {
        var args: backupRequestDto = {
          BackupLocation: this.backupLocation,
          DatabaseDocument: doc
        };
        this.post('/admin/backup', JSON.stringify(args), this.db, { dataType: 'text' })
          .fail(response=> result.reject(response))
          .done(()=> {
            this.getBackupStatus(result);
          });

      });

    return result;
  }

  private getBackupStatus(result: JQueryDeferred<any>) {

    new getDocumentWithMetadataCommand("Raven/Backup/Status", this.db)
      .execute()
      .fail(response=> result.reject(response))
      .done((backupStatus: backupStatusDto)=> {
        this.updateBackupStatus(backupStatus);
        if (backupStatus.IsRunning) {
          setTimeout(()=> this.getBackupStatus(result), 1000);
        }
      });

  }

}

export = backupDatabaseCommand;
import commandBase = require("commands/commandBase");
import database = require("models/database");

class stopIndexingCommand extends commandBase {

  constructor(private db: database, private backupLocation: string) {
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
          .fail(response=> {
            debugger
            result.reject(response);
          })
          .done(()=> {
            debugger
            var hasCompleted = false;
            while (!hasCompleted) {
              this.query("Raven/Backup/Status", null, this.db)
                .done((backupStatus: backupStatusDto)=> {
                  debugger
                });
            }
          });
      });

    return result;
  }

}

export = stopIndexingCommand;